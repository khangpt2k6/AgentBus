{{/*
Expand the name of the chart.
*/}}
{{- define "eventbus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name. Truncated to 63 chars (DNS-1123).
*/}}
{{- define "eventbus.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label (with version).
*/}}
{{- define "eventbus.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "eventbus.labels" -}}
helm.sh/chart: {{ include "eventbus.chart" . }}
{{ include "eventbus.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: eventbus
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "eventbus.selectorLabels" -}}
app.kubernetes.io/name: {{ include "eventbus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "eventbus.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "eventbus.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Resolved image (repository + tag, falling back to AppVersion).
*/}}
{{- define "eventbus.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
PVC claim name - either an existing claim or one created by the chart.
*/}}
{{- define "eventbus.pvcName" -}}
{{- if .Values.persistence.existingClaim -}}
{{ .Values.persistence.existingClaim }}
{{- else -}}
{{ include "eventbus.fullname" . }}-data
{{- end -}}
{{- end }}
