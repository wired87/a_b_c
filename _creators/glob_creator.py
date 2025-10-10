import asyncio

import ray
from cluster_nodes.cluster_utils.G import UtilsWorker
from cluster_nodes.cluster_utils.base import BaseActor
from cluster_nodes.cluster_utils.logger_worker import LoggerWorker
from cluster_nodes.head import Head
from cluster_nodes.state_handler.main import StateHandler
from cluster_nodes.state_handler.state_handler import StateHandlerWorker

@ray.remote
class GlobsMaster(BaseActor):

    def __init__(self, world_cfg):
        BaseActor.__init__(self)
        self.host = {}
        self.world_cfg = world_cfg
        self.alive_workers = []
        self.available_actors = {
            "HEAD": self.create_head_server,
            "GLOB_LOGGER": self.create_global_logger,
            "GLOB_STATE_HANDLER": self.create_global_state_handler,
            "UTILS_WORKER": self.create_utils_worker,
        }
        self.sh = StateHandler(
            host=self.host,
            run=True
        )
        print("GlobsMaster initiaized")

    def create(self):
        try:
            self.create_globs()
            id_map = list(self.host.keys())
            print("id_map", id_map)
            self.sh.await_alive(
                id_map=id_map
            )
            print("GlobMaster Creation procedure finished")
            asyncio.run(self.sh.distiribute_host(
                host=self.host.copy()
            ))
            print("Exit GlobsMaster...")
        except Exception as e:
            print(f"Err GlobCreator.create: {e}")
        ray.actor.exit_actor()

    def create_globs(self):
        print(f"Create GLOBS")
        retry = 3
        for name in list(self.available_actors.keys()):
            for i in range(retry):
                print(f"Create: {name} \nTry: {i}")
                try:
                    self.available_actors[name](name)
                    break
                except Exception as e:
                    print(f"Err: {e}")
        print("GLOB creation finished")

    def create_utils_worker(self, name):
        ref = UtilsWorker.options(
            name=name,
            lifetime="detached",
        ).remote(
            world_cfg=self.world_cfg
        )
        self.host[name] = ref

    def create_global_logger(self, name):
        ref = LoggerWorker.options(
            name=name,
            lifetime="detached"
        ).remote(
            host=self.host
        )
        self.host[name] = ref


    def create_global_state_handler(self, name):
        ref = StateHandlerWorker.options(
            name=name,
            lifetime="detached",
        ).remote(
            host=self.host,
        )
        self.host[name] = ref


    def create_head_server(
            self,
            name
    ):
        ref = Head.options(
            name=name,
            lifetime="detached",
        ).remote()
        self.host[name] = ref
        print("✅ Head started successfully")


if __name__ == "__main__":
    ref = GlobsMaster.remote()
