import asyncio

import ray
from ray import serve

from a_b_c._creators.utils import CloudRcsCreator
from a_b_c.bq_agent.bq_agent import BQService
from a_b_c.gemw.gem import WGem
from app_utils import ENV_ID
from cluster_nodes.cluster_utils.base import BaseActor
from cluster_nodes.cluster_utils.db_worker import FBRTDBAdminWorker
from a_b_c.spanner_agent.spanner_agent import SpannerWorker
from god.god import God
from qf_core_base.qf_utils.all_subs import ALL_SUBS
from utils.utils import Utils

@ray.remote
class CloudMaster(Utils, BaseActor):
    """
    todo create one universal creator worker and cloudupdaor workers -> connect ot cli (+docs)) e.g.: gem worker holds vector store (vs) with fetched docs (or uses google api to fetch -> process (like convert to G format) -> embed)
    Creates Core rcs:
    BQ
    - DB : ENV ID
    - Tables for time series-> create for each nid a table to avoid persistency issues
    SP
    - instance
    - db
    - tables (just for states -> classified in ntypes)

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
        self.bq_tables=[]

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


    def get_bq_table_names_to_create(self):
        amount_nodes: int = self.god.world_creator.px_creator.get_amount_nodes()
        for ntype in ALL_SUBS:
            for i in range(amount_nodes):
                self.bq_tables.append(
                    f"{ntype}_px_{i}"
                )
        return self.bq_tables







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
        self.crcs_creator = CloudRcsCreator(host=self.host)

        all_bq_tables = self.get_bq_table_names_to_create()
        await asyncio.gather(
            *[
                self.crcs_creator.create_spanner_rcs(),
                self.crcs_creator.create_bq_rcs(
                    tables_to_create=all_bq_tables
                )
            ]
        )

        await self.god.main()

        await self.create_sgraph()

        self.await_actor("HEAD")
        ray.get_actor(name="HEAD").handle_initialized.remote(
            {"RELAY": ref}
        )
        ray.actor.exit_actor()


    async def create_sgraph(self):
        # Upsert Pixel payload
        payload = {
            "graph_name": ENV_ID,
            "node_tables": ALL_SUBS,
            "edge_tables": ["EDGES"],
        }

        # upsert
        await self.apost(
            url=f"{self.domain}{self.sp_create_graph_endp}",
            data=payload
        )



























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
