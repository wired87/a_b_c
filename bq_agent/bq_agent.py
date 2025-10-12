

"""

Workflow
stsart bq actor at inti keep endpoit global


"""

import asyncio
from google.cloud import bigquery
from fastapi.responses import StreamingResponse
import io, csv

from ray import serve

from a_b_c.bq_agent._bq_core.loader.aloader import ABQHandler
from app_utils import APP, ENV_ID
from cluster_nodes.cluster_utils.base import BaseActor

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_cpus": .4},
    max_ongoing_requests=1000
)
@serve.ingress(APP)
class BQService(BaseActor):
    def __init__(self):
        BaseActor.__init__(self)
        self.abq = ABQHandler(dataset=ENV_ID)

    @APP.post("/create-table")
    async def create_table(
            self,
            data:dict
    ):
        # todo create single query for all tables
        print("=========== create-table ===========")
        table_names=data["table_names"]
        ttype = data["table_names"]
        self.abq.aget_create_bq_table(
            table_names=table_names,
            ttype=ttype
        )
        print("created tables with success")

    @APP.post("/create-database")
    async def create_database(self, data: dict):
        print("=========== create-database ===========")
        db_name = data["db_name"]
        print("db name:", db_name)
        dataset = bigquery.Dataset(
            self.abq.get_ds_ref()
        )
        try:
            dataset.location = "US"

            # Use the client object to send the dataset configuration to the API
            created_dataset = self.abq.bqclient.create_dataset(dataset)
            print(f"Created dataset {created_dataset.full_dataset_id}")
        except Exception as e:
            # Handle error, e.g., if the dataset already exists (Conflict) or name is invalid
            print(f"Error creating dataset: {e}")

    @APP.post("/download/{table_name}")
    async def download_table(self, table_name: str, limit: int = 1000):
        """
        Fetches table data from BigQuery, converts to CSV, and returns as downloadable file.
        """
        query = f"SELECT * FROM `{self.abq.pid}.{self.abq.ds_id}.{table_name}` LIMIT {limit}"
        rows = self.abq.run_query(query, conv_to_dict=True)

        if not rows:
            return {"error": f"No data found in table {table_name}"}

        # CSV in Memory schreiben
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)

        # StreamingResponse mit CSV Header zurückgeben
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={table_name}.csv"}
        )

    @APP.post("/upsert")
    async def upsert(self, request):
        payload = await request.json()
        table = payload["table"]
        rows = payload["rows"]
        asyncio.create_task(self.abq.async_write_rows(table=table, rows=rows))
        return {"status": "ok", "rows": len(rows)}

    @APP.post("/query")
    async def query(self, request):
        payload = await request.json()
        q = payload["query"]
        result = self.abq.run_query(q, conv_to_dict=True)
        return {"result": result}

    @APP.post("/ensure_table")
    async def ensure_table(self, request):
        payload = await request.json()
        table = payload["table"]
        rows = payload.get("rows", [])
        ref, schema = self.abq.ensure_table_exists(table, rows)
        return {"table_ref": ref, "schema": [s.to_api_repr() for s in schema]}
