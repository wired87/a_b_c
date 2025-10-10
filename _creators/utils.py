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

        self.domain = "http://127.0.0.1:8000" if os.name == "nt" else f"https://{DOMAIN}"

    async def create_spanner_rcs(self):
        print("============== CREATE SPANNER RCS ===============")
        data = await self.get_sp_create_payload()
        for endpoint, payload in data.items():
            if isinstance(payload, list):
                await asyncio.gather(*[
                    self.create_sp_rcs(endpoint, p)
                    for p in payload
                ])
            elif isinstance(payload, dict):
                await self.create_sp_rcs(endpoint, payload)
        print("All Spanner rcs created successfully")


    async def create_sp_rcs(self, endpoint, payload):
        response = await self.apost(
            url=f"{self.domain}{endpoint}",
            data=payload,
        )
        print("response.data", response.data)
        if response.ok:
            return True
        else:
            # todo error intervention
            return False


    async def create_bq_rcs(self, tables_to_create):
        print("============== CREATE BQ RCS ===============")
        data = self.get_bq_create_payload(tables_to_create)
        for endpoint, data in data.items():
            response = await self.apost(
                url=f"{self.domain}{endpoint}",
                data=data,
            )
            print("response.data", response.data)


    async def get_node_schema(self):
        obj_ref = ray.get(self.host["UTILS_WORKER"].get_nodes_each_type.remote())
        schema_ref = await self.apost(
            url=f"{self.domain}/extract-schema/",
            data={"type": "node", "obj_ref": obj_ref}
        )
        # unpack data
        schema = ray.get(schema_ref)
        return schema

    async def get_edge_schema(self, obj_ref):
        # obj_ref: ObjectRef to all edges
        schema_ref = await self.apost(
            url=f"{self.domain}/extract-schema/",
            data={
                "type": "edge",
                "obj_ref": obj_ref
            }
        )
        #unpack data
        schema = ray.get(schema_ref)
        return schema



    async def get_sp_create_payload(self):
        edge_obj_ref, eids = self.get_edge_data()

        # Fetch schema from data[list]
        schemas = await asyncio.gather(
            *[
                self.get_edge_schema(obj_ref=edge_obj_ref),
                self.get_node_schema(),
            ]
        )

        G_ref = ray.get(self.host["UTILS_WORKER"].get_G.remote())

        data = {
            "/create-instance": dict(
                instance_id=f"I_{ENV_ID}"
            ),
            "/create-rcs": dict(
                instance_id=f"I_{ENV_ID}",
                node_table_map=[*ALL_SUBS, "PIXEL"],
                edge_table_map=eids,
                edge_table_schema=schemas[0],
                node_table_schema=schemas[1],
                graph_name=f"G_{ENV_ID}",
            ),
            "/create-change-stream": dict(
                table_names=ALL_SUBS,
                stream_type="node",
            ),
        }
        return data

    def get_edge_data(self):
        print("RELAY: Get edges")
        edge_refs = ray.get(
            ray.get_actor(name="UTILS_WORKER").get_all_edges.remote(
                just_id=False,
            )
        )

        edges: list[dict] = ray.get(edge_refs)
        eids = [eid.get("id").upper() for eid in edges]
        new_obj_ref = ray.put(edges)
        return new_obj_ref, eids


    def get_bq_create_payload(self, tables_to_create:list):
        """
        Database
        Tables
        """
        data = {
            "/create-database": dict(
                db_name=ENV_ID
            ),
            "/create-table": dict(
                table_names=[*tables_to_create, "PIXEL"]
            ),
        }
        return data

