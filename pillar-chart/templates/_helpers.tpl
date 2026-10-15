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
{{- $domain := .Values.clusterDomain -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $cluster := list -}}
{{- range $i := until $replicas -}}
  {{- $member := printf "%s-etcd-%d=http://%s-etcd-%d.%s-etcd.%s.svc.%s:2380" $fullname $i $fullname $i $fullname $namespace $domain -}}
  {{- $cluster = append $cluster $member -}}
{{- end -}}
{{- join "," $cluster -}}
{{- end }}

{{/*
Generate etcd endpoints for NameNode
*/}}
{{- define "pillar.etcdEndpoints" -}}
{{- if not .Values.etcd.enabled -}}
none
{{- else -}}
{{- $fullname := include "pillar.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $domain := .Values.clusterDomain -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $endpoints := list -}}
{{- range $i := until $replicas -}}
  {{- $endpoint := printf "%s-etcd-%d.%s-etcd.%s.svc.%s:2379" $fullname $i $fullname $namespace $domain -}}
  {{- $endpoints = append $endpoints $endpoint -}}
{{- end -}}
{{- join "," $endpoints -}}
{{- end -}}
{{- end }}

{{/*
Kerberos: the realm, checked.
*/}}
{{- define "pillar.realm" -}}
{{- required "security.kerberos.realm is required with security.kerberos.enabled" .Values.security.kerberos.realm -}}
{{- end }}

{{/*
The NameNode's host: its Service's name, the name Hadoop clients derive the
NameNode's principal from (nn/_HOST).
*/}}
{{- define "pillar.namenodeHost" -}}
{{- printf "%s-namenode.%s.svc.%s" (include "pillar.fullname" .) .Release.Namespace .Values.clusterDomain -}}
{{- end }}

{{- define "pillar.namenodePrincipal" -}}
{{- printf "nn/%s@%s" (include "pillar.namenodeHost" .) (include "pillar.realm" .) -}}
{{- end }}

{{/*
Secrets with the NameNode's and the DataNodes' keytabs (and krb5.conf).
*/}}
{{- define "pillar.namenodeKeytabSecret" -}}
{{- if .Values.security.kerberos.operator.enabled -}}
{{- printf "%s-namenode-keytab" (include "pillar.fullname" .) -}}
{{- else -}}
{{- required "security.kerberos.namenodeSecret is required without the operator" .Values.security.kerberos.namenodeSecret -}}
{{- end -}}
{{- end }}

{{- define "pillar.datanodeKeytabSecret" -}}
{{- if .Values.security.kerberos.operator.enabled -}}
{{- printf "%s-datanode-keytab" (include "pillar.fullname" .) -}}
{{- else -}}
{{- required "security.kerberos.datanodeSecret is required without the operator" .Values.security.kerberos.datanodeSecret -}}
{{- end -}}
{{- end }}

{{/*
Arguments the NameNode and the DataNodes share: RPC protection and data
transfer encryption.
*/}}
{{- define "pillar.securityArgs" -}}
{{- with .Values.security.rpcProtection }}
- --rpc-protection={{ . }}
{{- end }}
{{- if .Values.security.encryptDataTransfer }}
- --encrypt-data-transfer
{{- end }}
{{- end }}
