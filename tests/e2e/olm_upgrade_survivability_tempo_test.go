package e2e_test

import (
	"fmt"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega
)

// assertTempoTraceHealth validates the live trace path without changing the
// Monitoring spec: Tempo must already be configured, accept a trace through
// the collector, and return that trace through its authenticated gateway.
func (tc *MonitoringTestCtx) assertTempoTraceHealth(
	t *testing.T, report *olmUpgradeSurvivabilityReport, stage string,
) {
	t.Helper()
	tc = tc.WithT(t)
	failureMsg := fmt.Sprintf("Tempo should accept and return traces %s", stage)
	defer func() {
		if t.Failed() && report.FailedAssertion == "" {
			report.FailedAssertion = failureMsg
		}
	}()

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.traces != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionTempoAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionOpenTelemetryCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("%s; configure traces before running a Tempo upgrade", failureMsg),
	)

	gatewayName := tc.readyTempoGateway(t, failureMsg)
	tc.probeTempoTraceRoundTrip(t, gatewayName, failureMsg)
}

func (tc *MonitoringTestCtx) readyTempoGateway(t *testing.T, failureMsg string) string {
	t.Helper()
	for _, candidate := range []struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		{gvk: gvk.TempoMonolithic, name: TempoMonolithicName},
		{gvk: gvk.TempoStack, name: TempoStackName},
	} {
		_, err := tc.fetchResource(t, candidate.gvk, types.NamespacedName{Name: candidate.name, Namespace: tc.MonitoringNamespace})
		if k8serr.IsNotFound(err) {
			continue
		}
		if err != nil {
			t.Fatalf("%s: read %s %s: %v", failureMsg, candidate.gvk.Kind, candidate.name, err)
		}
		tc.EnsureResourceExists(
			WithMinimalObject(candidate.gvk, types.NamespacedName{Name: candidate.name, Namespace: tc.MonitoringNamespace}),
			WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
			WithCustomErrorMsg("%s: %s %s is not Ready", failureMsg, candidate.gvk.Kind, candidate.name),
		)
		return "tempo-" + candidate.name + "-gateway"
	}
	t.Fatalf("%s: no TempoMonolithic or TempoStack operand exists in %s", failureMsg, tc.MonitoringNamespace)
	return ""
}

func tempoTracePayload(traceID, spanID string, startTime, endTime int64) string {
	return fmt.Sprintf(`{"resourceSpans":[{`+
		`"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"olm-upgrade-survivability"}}]},`+
		`"scopeSpans":[{"spans":[{"traceId":"%s","spanId":"%s","name":"olm-upgrade-survivability",`+
		`"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`,
		traceID, spanID, startTime, endTime)
}

func (tc *MonitoringTestCtx) probeTempoTraceRoundTrip(t *testing.T, gatewayName, failureMsg string) {
	t.Helper()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	traceID := fmt.Sprintf("%032x", time.Now().UnixNano())
	spanID := fmt.Sprintf("%016x", time.Now().UnixNano())
	probeNamespace := "olm-tempo-probe-" + suffix
	queryName := "olm-tempo-query-" + suffix
	serviceAccountName := "default"
	roleName := queryName

	tc.createFixtureResource(t, gvk.Namespace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: probeNamespace,
		Labels: map[string]string{
			otlpNamespaceLabel: "true",
		},
	}})
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: probeNamespace}), WithIgnoreNotFound(true))
	})

	now := time.Now().UnixNano()
	payload := tempoTracePayload(traceID, spanID, now, now+1_000_000)
	collectorEndpoint := fmt.Sprintf("http://data-science-collector-collector.%s.svc.cluster.local:4318/v1/traces", tc.MonitoringNamespace)
	ingestScript := fmt.Sprintf(`set -eu
status=$(curl --noproxy '*' -sS -o /tmp/response -w '%%{http_code}' \
  -H 'Content-Type: application/json' --data-binary %q %q)
cat /tmp/response
test "$status" = 200`, payload, collectorEndpoint)
	ingestPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ingest", Namespace: probeNamespace, Labels: otlpCallerNetworkPolicyLabels("llminferenceservice-workload")},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: new(false), SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name: "curl", Image: fakeGCSClientImage, Command: []string{"/bin/sh", "-c"}, Args: []string{ingestScript},
				SecurityContext: fixtureContainerSecurityContext(), TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, ingestPod)
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: ingestPod.Name, Namespace: ingestPod.Namespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded"`)),
		WithEventuallyTimeout(5*time.Minute),
		WithCustomErrorMsg("%s: OTLP trace ingestion probe failed", failureMsg),
	)

	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{Name: roleName}), WithIgnoreNotFound(true))
		tc.DeleteResource(WithMinimalObject(gvk.ClusterRole, types.NamespacedName{Name: roleName}), WithIgnoreNotFound(true))
	})

	tc.createFixtureResource(t, gvk.ClusterRole, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"tempo.grafana.com"}, Resources: []string{tc.MonitoringNamespace},
			ResourceNames: []string{"traces"}, Verbs: []string{"get"},
		}},
	})
	tc.createFixtureResource(t, gvk.ClusterRoleBinding, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccountName, Namespace: tc.MonitoringNamespace}},
	})
	queryURL := fmt.Sprintf("https://%s.%s.svc.cluster.local:8080/api/traces/v1/%s/tempo/api/traces/%s",
		gatewayName, tc.MonitoringNamespace, tc.MonitoringNamespace, traceID)
	queryScript := fmt.Sprintf(`set -eu
token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
for attempt in $(seq 1 60); do
  status=$(curl --noproxy '*' -ksS -o /tmp/trace -w '%%{http_code}' -H "Authorization: Bearer $token" %q)
  if [ "$status" = 200 ]; then cat /tmp/trace; exit 0; fi
  sleep 5
done
printf 'last HTTP status=%%s\n' "$status"
cat /tmp/trace
exit 1`, queryURL)
	queryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: queryName, Namespace: tc.MonitoringNamespace, Labels: map[string]string{
			"instance": "data-science-perses", "app.kubernetes.io/name": "data-science-perses",
		}},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, ServiceAccountName: serviceAccountName, SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name: "curl", Image: fakeGCSClientImage, Command: []string{"/bin/sh", "-c"}, Args: []string{queryScript},
				SecurityContext: fixtureContainerSecurityContext(), TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
		},
	}
	tc.createFixtureResource(t, gvk.Pod, queryPod)
	t.Cleanup(func() {
		tc.DeleteResource(WithMinimalObject(gvk.Pod, types.NamespacedName{Name: queryName, Namespace: tc.MonitoringNamespace}), WithIgnoreNotFound(true))
	})
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: queryName, Namespace: tc.MonitoringNamespace}),
		WithCondition(jq.Match(`.status.phase == "Succeeded"`)),
		WithEventuallyTimeout(6*time.Minute),
		WithCustomErrorMsg("%s: trace %s was not queryable through Tempo", failureMsg, traceID),
	)
}
