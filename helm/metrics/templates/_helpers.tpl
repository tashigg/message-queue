{{/*
Expand the name of the chart.
*/}}
{{- define "foxmq-metrics.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "foxmq-metrics.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "foxmq-metrics.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "foxmq-metrics.labels" -}}
helm.sh/chart: {{ include "foxmq-metrics.chart" . }}
{{ include "foxmq-metrics.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "foxmq-metrics.selectorLabels" -}}
app.kubernetes.io/name: {{ include "foxmq-metrics.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "foxmq-metrics.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "foxmq-metrics.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "foxmq-metrics.publisher.jobName" }}
{{- printf "%s-publisher" (include "foxmq-metrics.name" . ) }}
{{- end }}

{{- define "foxmq-metrics.subscriber.jobName" }}
{{- printf "%s-subscriber" (include "foxmq-metrics.name" . ) }}
{{- end }}

{{- define "foxmq-metrics.influxDB.username" }}
{{- default "admin" .Values.influxDB.username }}
{{- end }}
