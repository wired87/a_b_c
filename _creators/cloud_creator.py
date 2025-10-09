import asyncio
import os

import ray
from ray import serve

from a_b_c.bq_agent.bq_agent import BQService
from a_b_c.gemw.gem import WGem
from app_utils import ENV_ID, DOMAIN
from cluster_nodes.cluster_utils.base import BaseActor
from cluster_nodes.cluster_utils.db_worker import FBRTDBAdminWorker
from a_b_c.spanner_agent.spanner_agent import SpannerWorker
from god.god import God
from qf_core_base.qf_utils.all_subs import ALL_SUBS
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
            if isinstance(payload, list):
                await asyncio.gather(*[
                    self.make_response(endpoint, p)
                    for p in payload
                ])
            elif isinstance(payload, dict):
                await self.make_response(endpoint, payload)
        print("All Spanner rcs created successfully")


    async def make_response(self, endpoint, payload):
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



    async def create_bq_rcs(self):
        print("============== CREATE BQ RCS ===============")
        data = self.get_bq_create_payload()
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
            "/load-init-state-db-from-nx": dict(
                nx_obj_ref=G_ref
            ),
            "/create-change-stream": [
                dict(
                    table_names=ALL_SUBS,
                    stream_type="node",
                ),
                dict(
                    table_names=["edges"],
                    stream_type="edge",
                )
            ],
        }
        return data

    async def get_edge_data(self):
        print("RELAY: Get edges")
        edge_refs = ray.get(self.host["UTILS_WORKER"].get_all_edges.remote(
            datastore=False,
            just_id=False,
        ))

        edges: list[dict] = ray.get(edge_refs)
        eids = [eid.get("id").upper() for eid in edges]
        new_obj_ref = ray.put(edges)
        return new_obj_ref, eids


    def get_bq_create_payload(self):
        """
        Database
        Tables
        """
        data = {
            "/create-database": dict(
                db_name=ENV_ID
            ),"/create-table": dict(
                table_names=[*ALL_SUBS, "PIXEL"]
            ),
        }
        return data


@ray.remote
class CloudMaster(Utils, BaseActor):
    """
    Creates Core rcs:
    BQ
    - DB
    - Tables for time series data (classify in ntypes)
    SP
    - instance
    - db
    - tables (just for states)


    AND
    uses God to create data


    Finals state = All daa inside all resources
    """
    def __init__(
            self,
            world_cfg,
            resources: list[str],
    ):
        self.available_actors = {
            "GEM": self.create_gem_worker,
            "FBRTDB": self.create_fbrtdb_worker,
            "SPANNER_WORKER": self.create_spanner_worker,
            "BQ_WORKER": self.create_bq_worker,
        }

        BaseActor.__init__(self)
        super().__init__()

        self.head = None
        self.host = {}

        self.resources = resources
        self.alive_workers = []

        self.world_cfg = world_cfg

        self.god = God(
            world_cfg
        )

    def create_gem_worker(self, name):
        ref = WGem.options(
            lifetime="detached",
            name=name,
        ).remote()
        if ref:
            self.host[name] = ref

    async def create_actors(self):
        for actor_id in self.resources:
            self.create_worker(
                name=actor_id,
            )
        self.await_alive(
            total_workers=self.host
        )
        self.await_actor(actor_name="HEAD")
        ray.get_actor(name="HEAD").handle_initialized.remote(
            self.host
        )
        await self.create_rcs_wf()
        print("Exit CloudCreator...")



    async def create_rcs_wf(self):
        """
        Create Cloud Acotrs, resources and fill them
        """
        self.crcs_creator= CloudRcsCreator(host=self.host)
        await asyncio.gather(
            *[
                self.crcs_creator.create_spanner_rcs(),
                self.crcs_creator.create_bq_rcs()
            ]
        )

        await self.god.main()



        self.await_actor("HEAD")
        ray.get_actor(name="HEAD").handle_initialized.remote(
            {"RELAY": ref}
        )
        ray.actor.exit_actor()































    def create_worker(self, name):
        print(f"Create worker {name}")
        retry = 3
        for i in range(retry):
            try:
                # Remove __px_id form name (if)
                ref = self.available_actors[name](name)
                return ref
            except Exception as e:
                print(f"Err: {e}")

    def create_spanner_worker(self, name):
        ref = serve.run(SpannerWorker.options(
            name=name,
        ).bind(),
            name=name,
            route_prefix=f"/sp"
        )
        print("SPANNER worker deployed")
        self.host[name] = ref

    def create_bq_worker(self, name):
        ref = serve.run(
            BQService.options(
                name=name,
            ).bind(),
                name=name,
        )
        print("BigQUERY worker deployed")
        self.host[name] = ref

    def create_fbrtdb_worker(self, name):
        ref = FBRTDBAdminWorker.options(
            name=name,
            lifetime="detached"
        ).remote()
        self.host[name] = ref
        # Build G from data
        return ref



if __name__ == "__main__":
    ref = CloudMaster.remote(
        resources=dict(
            SPANNER_WORKER=dict(),
            BQ_WORKER=dict()
        ),
        head=None
    )
