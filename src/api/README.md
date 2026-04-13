# Nexus Fleet Integration API - Capa Web

Este directorio (`src/api`) aloja el microservicio REST construido con **FastAPI**. 
Su próposito fundamental es exponer públicamente las capacidades del **Nexus MVP** (nuestra lógica de Base de Datos y Limpieza) para que pueda conectarse con aplicaciones de terceros, ERPs, o integraciones móviles.

### El Archivo Principal: `main.py`
Es el corazón de nuestra capa HTTP. Arranca la inicialización segura de la base de datos de manera agnóstica (`init_db()` en su evento *startup*) y genera la auto-documentación técnica (Swagger OpenApi) de todo lo que se programe aquí.

---

## Detalle y Contexto de los Endpoints

A continuación, la función explicada y contextualizada de cada ruta disponible en esta API:

### 1. Ingesta Masiva y Limpieza Continua (ETL Remoto)
* **`POST /api/sync`**
  Este es el punto más agresivo del proyecto. Acepta de forma asíncrona grandes cargas (el modelo `SyncPayload` con diccionarios de maquinaria, empresas, y alquileres simultáneos). 
  **¿Qué hace?** Filtra los lotes usando nuestra clase `DataCleaner` para purificar los datos entrantes (quitando fechas mal formadas, parificando minúsculas, detectando horas irreales). Tras retener los inservibles en memoria, intenta volcar exclusivamente la "data limpia" en la BBDD a través del ORM, rechazando contratos de alquiler a los que les llegue a faltar la Integridad Referencial (las *Foreign Keys* de cliente y máquina). Devuelve como respuesta un JSON exhaustivo detallando qué entró, qué rebotó y qué se bloqueó de cada entidad en la carga.

### 2. Microservicios de Flota (Machinery)
* **`GET /api/machinery`**
  Emite el catálogo completo de máquinas existentes registradas en la base de datos `nexus_master.db`. 
  **Filtrado:** Admite utilizar los Query Params para cruzar consultas útiles, por ejemplo: `GET /api/machinery?status=disponible` devolverá inmediatamente un subconjunto iterado solo con las de dicho estado, listo para que un comercial lo consuma.

* **`GET /api/machinery/{vin}`**
  Un endpoint de "puntería" para consultar específicamente sobre un elemento a través de su identificador único industrial (el *VIN*). Lanzará una excepción limpia (`404 Not Found`) protegiendo al agente exterior de caídas del código en caso de un mapeo erróneo o inventado de la máquina objetivo.

### 3. Directorio de Clientes
* **`GET /api/companies`**
  Emite directamente el padrón general empresarial (Clientes B2B) asociados por CIF, reflejado limpiamente y formateado en JSON mediante la serialización nativa del Pydantic acoplado al ORM de `SQLModel`. Útil para un frontend de administración.

### 4. Gestor de Operaciones (Rentals)
* **`GET /api/rentals/active`**
  Filtra y emite instantáneamente aquellos contratos de maquinaria que siguen en pista, es decir, cuyos alquileres aún tienen el listado de devoluciones vacante (`return_date` == *None*). Al interactuar con el ORM local en su backend, escupirá no solo IDs, sino todas las métricas nativas ligadas (sabiendo a qué empresa o máquina pertenece ese identificador por medio de los Relationships del motor).

---

## ¿Cómo Levantarlo Exclusivamente?
Para poner en marcha esta documentación o habilitar localmente las peticiones REST, ejecuta la raíz asíncrona dentro del ecosistema `uv` mediante `uvicorn`:

```bash
uv run uvicorn src.api.main:app --reload
```
Una vez activo, su interfaz gráfica interactiva nativa estará operativa visualmente bajo la URI `http://127.0.0.1:8000/docs`.
