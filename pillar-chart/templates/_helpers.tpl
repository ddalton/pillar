{{/*
Expand the name of the chart.
*/}}
{{- define "pillar.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pillar.fullname" -}}
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
{{- define "pillar.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pillar.labels" -}}
helm.sh/chart: {{ include "pillar.chart" . }}
{{ include "pillar.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "pillar.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pillar.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Generate etcd initial cluster string for StatefulSet
*/}}
{{- define "pillar.etcdInitialCluster" -}}
{{- $fullname := include "pillar.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $cluster := list -}}
{{- range $i := until $replicas -}}
  {{- $member := printf "%s-etcd-%d=http://%s-etcd-%d.%s-etcd.%s.svc.cluster.local:2380" $fullname $i $fullname $i $fullname $namespace -}}
  {{- $cluster = append $cluster $member -}}
{{- end -}}
{{- join "," $cluster -}}
{{- end }}

{{/*
Generate etcd endpoints for NameNode
*/}}
{{- define "pillar.etcdEndpoints" -}}
{{- $fullname := include "pillar.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $endpoints := list -}}
{{- range $i := until $replicas -}}
  {{- $endpoint := printf "%s-etcd-%d.%s-etcd.%s.svc.cluster.local:2379" $fullname $i $fullname $namespace -}}
  {{- $endpoints = append $endpoints $endpoint -}}
{{- end -}}
{{- join "," $endpoints -}}
{{- end }}
