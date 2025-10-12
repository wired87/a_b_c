import asyncio
import os

import ray

from app_utils import ENV_ID, DOMAIN
from qf_utils.all_subs import ALL_SUBS
from utils.utils import Utils


class CloudRcsCreator(Utils):

    def __init__(self, host):
        super().__init__()
        self.host = host

        self.domain = "http://127.0.0.1:8001" if os.name == "nt" else f"https://{DOMAIN}"

    async def create_spanner_rcs(self):
        print("============== CREATE SPANNER RCS ===============")
        data = await self.get_sp_create_payload()
        for endpoint, payload in data.items():
            try:
                if isinstance(payload, list):
                    await asyncio.gather(*[
                        self.create_sp_rcs(endpoint, p)
                        for p in payload
                    ])

                elif isinstance(payload, dict):
                    await self.create_sp_rcs(endpoint, payload)
            except Exception as e:
                print(f"Err create_spanner_rcs: {e}")
        print("All Spanner rcs created successfully")


    async def create_sp_rcs(self, endpoint, payload):
        await self.apost(
            url=f"{self.domain}{endpoint}",
            data=payload,
        )
        print(f"{endpoint} request finished")


    async def create_bq_rcs(self, tables_to_create):
        print("============== CREATE BQ RCS ===============")
        data = self.get_bq_create_payload(tables_to_create)
        for endpoint, data in data.items():
            try:
                await self.apost(
                    url=f"{self.domain}{endpoint}",
                    data=data,
                )
                print(f"{endpoint} finalized")
            except Exception as e:
                print(f"Err create_bq_rcs: {e}")


    async def get_node_schema(self):
        # extract node attrs of ferm, gauge and h each
        node_attrs = ray.get(ray.get_actor(
            name="UTILS_WORKER"
        ).get_node_sum.remote())

        schema = await self.apost(
            url=f"{self.domain}/sp/extract-schema/",
            data={"attrs": node_attrs}
        )
        # unpack data
        return schema

    async def get_edge_schema(self):
        # obj_ref: ObjectRef to all edges
        edge_attrs = ray.get(ray.get_actor(
            name="UTILS_WORKER"
        ).get_all_edge_attrs.remote())
        schema = await self.apost(
            url=f"{self.domain}/sp/extract-schema/",
            data={
                "attrs": edge_attrs
            }
        )
        return schema



    async def get_sp_create_payload(self):
        data = {
            "/sp/create-instance": dict(
                instance_id=f"I_{ENV_ID}"
            ),
            "/sp/create-table": [
                dict(
                    table_name_map=[*ALL_SUBS, "PIXEL"],
                    table_schema=await self.get_node_schema(),
                ),
                dict(
                    table_name_map=["EDGES"],
                    table_schema=await self.get_edge_schema(),
                ),
            ],
            "/sp/create-change-stream": dict(
                table_names=ALL_SUBS,
                stream_type="node",
            ),
        }
        return data


    def get_edge_data(self):
        print("RELAY: Get edges")
        edge_attrs = ray.get(
            ray.get_actor(
                name="UTILS_WORKER"
            ).get_all_edge_attrs.remote()
        )
        return edge_attrs


    def get_bq_create_payload(self, tables_to_create:list):
        """
        Database
        Tables
        """
        data = {
            "/bq/create-database": dict(
                db_name=ENV_ID
            ),
            "/bq/create-table": dict(
                table_names=[
                    *tables_to_create,
                    "PIXEL"
                ],
                ttype="node",
            ),
        }
        return data

