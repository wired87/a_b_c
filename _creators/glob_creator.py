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

    def __init__(self):
        BaseActor.__init__(self)
        self.host = {}
        self.alive_workers = []
        self.available_actors = {
            "GLOB_LOGGER": self.create_global_logger,
            "GLOB_STATE_HANDLER": self.create_global_state_handler,
            "UTILS_WORKER": self.create_utils_worker,
            "HEAD": self.create_head_server,
        }
        self.sh = StateHandler(
            host=self.host,
            run=True
        )
        self.create()
        print("GlobsMaster initiaized")
    
    def create(self):
        #for actor_id in self.resources:
        self.create_globs()

        self.await_alive(total_workers=self.host)

        print("GlobMaster Creation procedure finished")
        asyncio.run(
            self.sh.distiribute_host(
                host=self.host.copy()
            )
        )
        print("Exit CloudCreator...")
        ray.actor.exit_actor()

    def create_globs(self):
        print(f"Create GLOBS")
        retry = 3
        for name, create_runnable in self.available_actors.items():
            for i in range(retry):
                print(f"Create: {name} \nTry: {i}")
                try:
                    # Remove __px_id form name (if)
                    ref = create_runnable(name)
                    return ref
                except Exception as e:
                    print(f"Err: {e}")
        print("GLOB creation finished")

    def create_utils_worker(self, name):
        ref = UtilsWorker.options(
            name=name,
            lifetime="detached",
        ).remote()
        self.host["UTILS_WORKER"] = ref
        return ref

    def create_global_logger(self, name):
        ref = LoggerWorker.options(
            name=name,
            lifetime="detached"
        ).remote(
            host=self.host
        )
        self.host["GLOB_LOGGER"] = ref
        return ref


    def create_global_state_handler(self, name):
        ref = StateHandlerWorker.options(
            name=name,
            lifetime="detached",
        ).remote(
            host=self.host,
        )
        self.host["GLOB_STATE_HANDLER"] = ref
        return ref


    def create_head_server(
            self,
            name
    ):
        ref = Head.options(
            name=name,
            lifetime="detached",
        ).remote()

        # ref = serve.get_deployment_handle(HEAD_SERVER_NAME, app_name=HEAD_SERVER_NAME)
        self.host["HEAD"] = ref
        print("✅ Head started successfully")
        return ref


if __name__ == "__main__":
    ref = GlobsMaster.remote()
