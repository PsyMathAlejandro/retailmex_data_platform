# Entregable 1: Preguntas al Stakeholder

> Antes de proponer cualquier arquitectura, es necesario clarificar las ambigüedades
> críticas del negocio. Las siguientes preguntas están dirigidas al **Director de
> Operaciones** y al **equipo de TI** de RetailMex, agrupadas por área de impacto.
> Por ello, propondé las siguientes preguntas de acuerdo distintos rubros para tener un mayor orden y contexto.

---

## 1. Volumen y SLA — Definen si necesitamos micro-batch o streaming real

**¿Cuántas tiendas físicas existen?**


**¿Cuál es el volumen aproximado de transacciones diarias combinando tiendas físicas y e-commerce?**


**Es necesario el SLA en 15 minutos o puede ser opcional**


**Supongamos que nuestro proceso falla y hay un retraso de dos horas, ¿Cómo podría afectar operativamente?**


**¿Hay categorías de producto con stock muy limitado donde una sobreventa sería inaceptable?**

---

## 2. Fuentes de Datos — Definen los patrones de ingesta

**¿Qué sistema de e-commerce están usando actualmente? ¿Expone una API REST, webhooks, o el acceso es directo a base de datos?**


**El ERP que maneja inventario central — ¿cuál es el mecanismo de integración disponible? ¿API, exportación de archivos, acceso directo a BD?**


**¿Existe algún identificador común de producto (SKU) entre el ERP, los POS y el e-commerce?**


**¿Cómo se diferencía entre venta por e-commerce y POS?**

---

## 3. Calidad de Datos — Definen las reglas de la capa Silver


**¿Qué tan frecuente es que los datos del SQL Server on-premise lleguen con registros duplicados, nulos o inconsistentes?**


**¿Hay algún proceso actual de reconciliación entre el inventario del ERP y los conteos físicos de tienda?**


**Con respecto a la pregunta anterior — ¿Después de dicha reconciliación actualizan el inventario?**


**¿Los datos de clientes del e-commerce contienen (nombre, email, dirección, datos de pago)?**

**¿Existe algún diseño de modelo de datos?**

---

## 4. Presupuesto y Restricciones — Filtran las opciones de arquitectura

**¿Tienen un presupuesto mensual estimado para la infraestructura cloud?**


**¿Hay restricciones de seguridad o compliance que impidan sacar datos del SQL Server on-premise hacia la nube directamente?**


**¿El equipo de TI tiene disponibilidad para dar soporte durante la migración o el proyecto de datos debe ser autosuficiente?**


**¿Cómo cifran los datos sencibles de los clientes de e-commerce?**


**En caso de ser necesario de cambiar de ERP o el sistema de punto de venta — ¿Hay algún contrato a tomar en cuenta?**

---

## 5. Roadmap y Crecimiento — Definen las decisiones de extensibilidad

**¿Cuántas tiendas físicas adicionales están planeando abrir en los próximos 12 meses?**


**¿Hay planes de agregar nuevas fuentes de datos en el corto plazo (marketplaces como MercadoLibre, sistemas de logística, redes sociales)?**


**¿Tienen claridad sobre qué herramienta de BI o visualización usarán para consumir los datos de la capa Gold?**


---

## Resumen de Prioridad

| Prioridad | Pregunta clave | Impacto si no se responde |
|---|---|---|
| 🔴 Crítica | SKU común entre sistemas | La consolidación de inventario puede ser inviable |
| 🔴 Crítica | Volumen de transacciones diarias | Arquitectura sobredimensionada o insuficiente |
| 🔴 Crítica | Presupuesto mensual cloud | Propuesta puede ser inviable económicamente |
| 🟡 Alta | SLA duro vs. deseable | Nivel de resiliencia y alertas mal calibrado |
| 🟡 Alta | Proveedor e integración e-commerce | Patrón de ingesta incorrecto para la fuente de mayor crecimiento |
| 🟡 Alta | PII y compliance | Riesgo legal si se omite desde el diseño |
| 🟢 Media | Roadmap de tiendas y fuentes | Decisiones de extensibilidad subóptimas |