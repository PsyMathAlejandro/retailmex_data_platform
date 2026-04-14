# RetailMex — Plataforma de Datos Moderna en AWS

> **Evaluación Técnica Sr. Data Engineer (IC4)**
> Stack: AWS · PySpark · CloudFormation · GitHub Actions · Databricks (Fase 2)

---

## Resumen

RetailMex opera 8 tiendas físicas en México y recientemente lanzó un canal de e-commerce con crecimiento del 40% mensual. El modelo de datos actual —un proceso batch nocturno hacia un Data Warehouse on-premise en SQL Server. Introduce un lag de hasta 18 horas, lo que imposibilita la toma de decisiones operativas en tiempo real.

Este repositorio contiene el diseño e implementación parcial de una **plataforma de datos moderna en AWS** que resuelve los siguientes objetivos de negocio:

| Objetivo | Solución propuesta |
|---|---|
| Visibilidad de inventario con latencia ≤ 15 min | Ingesta near real-time vía AWS Glue + S3 |
| Vista unificada de ventas físicas y e-commerce | Pipeline de consolidación en capa Gold (PySpark) |
| Arquitectura extensible para nuevas fuentes | Diseño medallion (Bronze / Silver / Gold) desacoplado |
| Costos de operación predecibles | Serverless-first: Glue, Lambda, S3 — sin servidores permanentes |
| Unificación y simplificación de pipelines (roadmap) | Migración planificada a Databricks sobre AWS en Fase 2 |

---

## Arquitectura Propuesta

> 📄 Ver detalle completo en [`docs/02_architectura.md`](./docs/02_arquitectura.md)

### Vista general

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS                            │
│  SQL Server (on-prem)   POS por tienda   E-commerce API   ERP       │
└────────────┬───────────────────┬──────────────┬──────────┬──────────┘
             │                   │              │          │
             ▼                   ▼              ▼          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CAPA DE INGESTA                              │
│         AWS Glue (JDBC batch)    +    AWS Lambda (webhooks/API)     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA LAKE EN S3 (Medallion)                       │
│                                                                      │
│   s3://retailmex-datalake/                                           │
│   ├── bronze/    ← datos crudos, sin transformar                    │
│   ├── silver/    ← datos limpios, validados, tipados                │
│   └── gold/      ← agregaciones de negocio listas para consumo     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CAPA DE PROCESAMIENTO                             │
│              AWS Glue Jobs (PySpark) orquestados por                │
│              AWS Step Functions + EventBridge Scheduler             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAPA DE CONSUMO                                │
│          AWS Athena (SQL ad-hoc)  +  Amazon QuickSight (BI)         │
│          AWS Glue Data Catalog (metadatos y gobernanza)             │
└─────────────────────────────────────────────────────────────────────┘
```

### Servicios AWS utilizados

| Servicio | Rol en la arquitectura |
|---|---|
| **S3** | Data Lake principal (Bronze / Silver / Gold) |
| **AWS Glue** | ETL jobs en PySpark + Catálogo de datos (Fase 1) |
| **AWS Lambda** | Ingesta event-driven (e-commerce webhooks) |
| **Step Functions** | Orquestación del pipeline |
| **EventBridge** | Scheduler de ejecuciones (cada 15 min) |
| **Athena** | Consultas SQL sobre el Data Lake |
| **IAM** | Roles con least-privilege por servicio |
| **CloudWatch** | Logs, métricas y alertas operativas |
| **Databricks on AWS** | Unificación de Glue Jobs + Delta Lake + Unity Catalog (Fase 2) |

---

## Estructura del Repositorio

```
retailmex_MEX_EVALUATION/
│
├── README.md                          ← este archivo
│
├── docs/
│   ├── 01_preguntas_stakeholders.md   ← preguntas previas al diseño
│   └── 02_architectura.md   ← diagrama y justificación detallada
│
├── code/
│    └──  silver_transform.py       ← limpieza y validación 
│   
├── infrastructura/
│   └── cloudformation.yaml          ← IaC: S3, Glue, IAM, Lambda
|
├── img/
│   └── diagrama_aws_retailMex.png    ← imagen de la arquitectura
│
└── .github/
    └── workflows/
        └── deploy.yml               ← CI/CD: lint + deploy automático
```

---

## Cómo Ejecutar

### Pre-requisitos

- AWS CLI configurado con perfil válido
- Python 3.9+
- Acceso al SQL Server on-premise 

### 1. Desplegar infraestructura

```bash
aws cloudformation deploy \
  --template-file infrastructure/cloudformation.yaml \
  --stack-name retailmex-data-platform \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev
```

### 2. Ejecutar pipeline manualmente

```bash
# Ingesta Bronze
aws glue start-job-run --job-name retailmex-bronze-ingestion

# Transformación Silver
aws glue start-job-run --job-name retailmex-silver-transform

# Agregación Gold
aws glue start-job-run --job-name retailmex-gold-aggregation
```

### 3. Verificar datos en Athena

```sql
-- Vista consolidada de ventas (físicas + e-commerce)
--Canal es sí es E-commerce o tienda física
SELECT canal, fecha, SUM(monto_total) as ventas_totales
FROM retailmex_gold.ventas_consolidadas
WHERE fecha = CURRENT_DATE
GROUP BY canal, fecha;
```

---

## Decisiones Técnicas y Trade-offs

### ¿Por qué Glue en Fase 1 y Databricks en Fase 2?

La Fase 1 usa **AWS Glue** por su naturaleza serverless: sin clusters permanentes que gestionar, ideal para un equipo de 2 personas y cargas predecibles. Es el punto de entrada más rápido y de menor fricción operativa.

Sin embargo, la arquitectura está diseñada desde el inicio para **migrar a Databricks sobre AWS** en una segunda fase. Los motivos:
- **Unificación**: múltiples Glue Jobs independientes (Bronze, Silver, Gold) se consolidan en un único workspace con orquestación nativa
- **Delta Lake**: reemplaza Parquet plano en S3 con ACID transactions, time travel y schema enforcement
- **Unity Catalog**: gobernanza centralizada de datos, linaje y control de acceso en un solo lugar
- **Developer experience**: notebooks colaborativos, debugging interactivo y mayor velocidad de iteración para el equipo

Esta decisión está alineada con el roadmap tecnológico del equipo de datos.

### ¿Por qué arquitectura Medallion (Bronze/Silver/Gold)?

Permite **reprocessing**: si hay un bug en la transformación Silver, los datos crudos en Bronze siguen intactos. También facilita agregar nuevas fuentes sin tocar capas superiores — directamente alineado con el objetivo de extensibilidad.

### ¿Por qué micro-batch cada 15 min y no streaming real?

El SLA de negocio es visibilidad en ≤ 15 minutos, no segundos. Streaming (Kinesis + Flink) agregaría complejidad operativa y costo significativo para un equipo pequeño. Si el negocio escala y el SLA se vuelve más estricto, la arquitectura puede evolucionar hacia Kinesis Data Streams sin rediseño mayor.

### ¿Qué queda fuera del scope de este entregable?

- Ingesta real del ERP (requiere definir API/conector con TI)
- Masking para datos de clientes de e-commerce
- Estrategia de disaster recovery entre regiones
- Capa de procesamiento Bronze y Gold
- Stack completo de CloudFormation

---

## Preguntas al Stakeholder

Antes de comenzar el diseño, se identificaron ambigüedades críticas en los requerimientos. Ver listado completo en [`docs/01_preguntas_stakeholders.md`](./docs/01_preguntas_stakeholders.md).

---

## Autor

**HERRERA GANDARELA** GABRIEL ALEJANDRO
