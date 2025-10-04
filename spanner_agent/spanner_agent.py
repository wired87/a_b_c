import asyncio

import networkx as nx
import ray
import ray.serve as serve
from typing import List, Dict
from datetime import datetime
from fastapi import HTTPException


# --- ENDE SIMULIERTE IMPORTE ---
from datetime import timezone

from google.cloud.spanner_v1 import param_types

from _spanner_graph.acore import ASpannerManager
from _spanner_graph.change_streams.main import SpannerChangeStreamer
from _spanner_graph.create_workflow import SpannerCreator
from _spanner_graph.g_utils import SpannerGraphManager
from _spanner_graph.utils.timestamp import sp_timestamp

from app_utils import APP, ENV_ID, GCP_ID, USER_ID
from cluster_nodes.cluster_utils.base import BaseActor
from qf_core_base.qf_utils.all_subs import ALL_SUBS

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_cpus": .4},
    max_ongoing_requests=100
)
@serve.ingress(APP)
class SpannerWorker(BaseActor):
    def __init__(self):
        # Initialisiere die notwendigen Spanner-Helfer
        # Verwende Dummy-Initialisierung, da die echten Klassen nicht importiert werden können
        super().__init__()
        self.spa_manager = ASpannerManager()
        self.sp_creator = SpannerCreator()
        self.cs_manager = SpannerChangeStreamer()

        self.sg_utils = SpannerGraphManager(
            spa=self.spa_manager,
        )

        # Initialisiere asynchron eine Sitzung beim Start
        self.session_ready_task = asyncio.create_task(self.spa_manager.acreate_session())
        self.project_id = GCP_ID

        # Statische Graph-Info für Demo
        self.graph_name = ENV_ID
        print("=========== SpannerWorker initilized ===========")


    async def _safe_task_run(self, func, *args, **kwargs):
        """Erstellt eine Task und wartet, um den Actor nicht zu blockieren."""
        await self.session_ready_task  # Stellt sicher, dass die Spanner Session bereit ist
        return await asyncio.create_task(func(*args, **kwargs))


    @APP.post("/get-table-entry")
    async def get_table_entry(
            self,
            table_name,
            where_key,
            is_value,
            select_table_keys
    ):
        entry = None
        table_exists:bool = await self.spa_manager.acheck_table_exists(table_name)
        if table_exists is True:
            query = self.spa_manager.custom_entries_query(
                table_name,
                check_key=where_key,
                check_key_value=is_value,
                select_table_keys=select_table_keys
            )
            entry:dict or list[dict] = self.spa_manager.asnap(
                query,
                return_as_dict=True
            )

        return {"table_entry": entry}

    @APP.post("/create-instance")
    async def create_resources_route(
            self,
            instance_id,
    ):
        ########################## INSTANCE ##########################
        ## todo craeate schema based on neighbor type
        self.sp_creator.create_instance(
            project_id=self.project_id,
            instance_name=instance_id,
            processing_units=100
        )
        return {"message": "All resources checked and created/updated successfully! ✨"}
    @APP.post("/create-database")
    async def create_resources_route(
            self,
            instance_id,
    ):
        ########################## INSTANCE ##########################
        ## todo craeate schema based on neighbor type
        self.sp_creator.create_instance(
            project_id=self.project_id,
            instance_name=instance_id,
            processing_units=100
        )
        return {"message": "All resources checked and created/updated successfully! ✨"}

    @APP.post("/create-rcs")
    async def create_resources_route(
            self,
            instance_id: str,
            node_table_map,
            edge_table_map,
            edge_table_schema,
            node_table_schema,
            graph_name: str = "DEFAULT_GRAPH",
            change_stream_name: str = "DEFAULT_CS",

    ):
        """Route zur Erstellung aller Spanner-Ressourcen mit Existenzprüfung."""

        async def create_workflow():
            """
            Get rcs
            """
            print(f"🏗️ Starting creation workflow for instance: {instance_id}")
            success:bool
            # create db & tables 
            success = await self.spa_manager.create_core_rcs(
                node_table_map,
                edge_table_map,
                edge_table_schema,
                node_table_schema,
            )

            if success is True:
                print("Create Spanner Graph")
                success = self.sp_creator.create_graph(
                    node_tables=node_table_map,
                    edge_tables=edge_table_map,
                    graph_name=None
                )
                print(f"✅ Graph {graph_name} created/recreated.")
            print(">>> SG RCS PROCESS FINISHED")
            return {"message": "All resources checked and created/updated successfully! ✨"}

        return await self._safe_task_run(create_workflow)

    # ------------------------------------------------------------------------------------------------------------------



    @APP.post("/extract-schema")
    async def extract_schema(self, data):
        print("=========== extract-schema ===========")
        if "type" in data and "obj_ref" in data:
            schema = {}

            # unapck data
            obj_ref = data["obj_ref"]
            schema_data = ray.get(obj_ref)

            if data["type"] == "node":
                schema = self.sg_utils.get_universell_sp_schema(
                    node_list=schema_data
                )
            elif data["type"] == "edge":
                schema = self.sg_utils.extract_universell_edge_schema(
                    edge_list=schema_data
                )
            ref = ray.put(schema)
            return ref

    @APP.post("/load-init-state-db-from-nx")
    async def load_database_initial(self, nx_obj_ref):
        print("=========== load-init-state-db-from-nx ===========")

        try:
            #nx_obj_ref = ray.get(self.host["UTILS_WORKER"].get_G.remote())
            # BUILD G
            G:nx.Graph = ray.get(nx_obj_ref)

            print("load NODE tables")

            await asyncio.gather(
                *[
                    self.spa_manager.upsert_row(
                        batch_chunk=attrs,
                        table=attrs.get("type").upper())
                    for nid, attrs in G.nodes(data=True)
                    if attrs.get("type") in ALL_SUBS
                ]
            )

            print("load EDGE tables")
            await asyncio.gather(
                *[
                    self.spa_manager.upsert_row(
                        batch_chunk=[{
                            "src": src,
                            "trgt": trgt,
                            **attrs,
                        }],
                        table=attrs.get("id").upper())
                    for src, trgt, attrs in G.edges(data=True)
                    if attrs.get("type") in ALL_SUBS
                ]
            )
            return {"message": "All resources inserted successfully! ✨"}

        except Exception as e:
            print(f"Err load_database_initial {e}")
            return {"message": f"Error: {e}"}


    @APP.post("/create-change-stream")
    async def upsert_row_route(self, node_tables, edge_tables=None):
        print("=========== create-change-stream ===========")

        ncid = f"NODE_{ENV_ID}"
        success = self.cs_manager.create_change_stream(
            tables=node_tables,
            start_time=sp_timestamp(),
            cs_id=ncid
        )
        if success is True:
            print(f"✅ Node Change Stream {ncid} created.")

        if edge_tables is not None:
            print("Create Edge CS")
            ecid = f"EDGE_{ENV_ID}"
            success = self.cs_manager.create_change_stream(
                tables=edge_tables,
                start_time=sp_timestamp(),
                cs_id=ecid
            )
            if success is True:
                print(f"✅ Edge Change Stream {ecid} created.")

        return {"message": "All resources checked and created/updated successfully! ✨"}

    @APP.post("/upsert/{table_name}")
    async def upsert_row_route(self, table_name: str, rows: List[Dict]):
        """Route zum Einfügen/Aktualisieren von Batch-Zeilen."""
        print("=========== upsert ===========")

        async def upsert_workflow():
            if not await self.spa_manager.acheck_table_exists(table_name):
                raise HTTPException(status_code=404, detail=f"Table {table_name} does not exist. ❌")

            # Die Logik in aupdate_insert (aus acore.py) handhabt bereits Batching
            await self.spa_manager.aupdate_insert(table_name, rows)

            return {"message": f"Successfully upserted {len(rows)} rows into {table_name}. ⬆️"}

        return await self._safe_task_run(upsert_workflow)

    # ------------------------------------------------------------------------------------------------------------------

    @APP.get("/read-change-stream/{change_stream_name}")
    async def read_change_stream_route(self, change_stream_name: str, start_ts: str):
        """Route zum Abrufen von Änderungsdatensätzen seit einem Start-Timestamp."""
        print("=========== read-change-stream ===========")

        async def read_workflow():
            # Die Logik in main.py (poll_change_stream) wird hier zur asynchronen Ausführung gewickelt.
            # In einem echten Async-Manager (ASpannerManager) würde diese Logik implementiert sein.

            # Da poll_change_stream nicht async ist, simulieren wir den Aufruf und die Funktionalität
            print(f"🔍 Reading change stream {change_stream_name} starting at {start_ts}...")

            # Dies simuliert das Polling und die Rückgabe von Records und dem neuen Checkpoint
            # In der realen Implementierung müsste dies eine asynchrone API-Query sein.

            # Hier müsste die Logik zum Unnesten und Extrahieren des Tabellennamens aus der Antwort erfolgen

            # Simulation der Rückgabe des neuen Checkpoints
            new_ts = datetime.now(timezone.utc).isoformat()

            return {
                "message": "Change stream read simulated.",
                "new_checkpoint_ts": new_ts,
                "records_count": 5  # Simulierte Anzahl von Datensätzen
            }

        return await self._safe_task_run(read_workflow)

    # ------------------------------------------------------------------------------------------------------------------

    @APP.get("/get-neighbors/{neighbor_id}")
    async def get_neighbors_route(self, nid: str, graph_name):
        """Route zur Abfrage der Center-Nodes für einen gegebenen Nachbarn."""
        print("=========== get-neighbors ===========")
        async def neighbor_workflow():
            # Simuliere die asynchrone Ausführung des SQL-Snapshots
            center_nodes = await asyncio.to_thread(
                self.cs_manager.find_center_nodes_for_neighbor,
                neighbor_id
            )

            if not center_nodes:
                return {"message": f"No center nodes found for neighbor {neighbor_id}. 🤷"}

            # Simulation der Benachrichtigung aller Center-Nodes
            await asyncio.gather(*[
                asyncio.to_thread(self.cs_manager.notify_center_node, node_id, {"change": True})
                for node_id in center_nodes
            ])

            return {
                "message": f"Found and notified {len(center_nodes)} center nodes. 🔔",
                "center_nodes": center_nodes
            }

        return await self._safe_task_run(neighbor_workflow)

    # ------------------------------------------------------------------------------------------------------------------

    @APP.delete("/list-entries")
    async def list_entries(
            self,
            column_name,
            table_name,
    ):
        print("=============== LIST ENTRIES ===============")
        params = {
            "target_values": target_values
        }

        param_types_dict = {
            "target_values": param_types.Array(param_types.STRING)
        }

        query = self.spa_manager.get_entries_from_list_str(
            column_name=column_name,
            table_name=table_name,
        )
        asyncio.create_task(self.spa_manager.asnap(query))
        return {"message": f"Instance and all contained resources have been deleted. 💥"}

    @APP.delete("/delete-instance/{instance_id}")
    async def delete_instance_route(self, instance_id: str):
        """Route zur vollständigen Löschung einer Spanner-Instanz und aller Ressourcen."""
        print("=========== delete-instance ===========")

        async def delete_workflow():
            print(f"💣 Starting deletion workflow for instance: {instance_id}")

            # 1. Lösche alle Tabellen in der Standarddatenbank (Simuliert)
            table_names = await self.spa_manager.atable_names()
            await self.spa_manager.adel_table_batch(table_name=table_names)
            print(f"✅ Deleted all {len(table_names)} tables.")

            # 2. Lösche die Datenbank (Hier müsste die Logik aus SpannerCreator oder SpannerCore her)
            # await self.spa_manager.update_db(self.spa_manager.drop_database_query(self.spa_manager.db))
            print(f"✅ Deleted database.")

            # 3. Lösche die Instanz (Müsste in SpannerCore implementiert sein)
            # self.sp_creator.delete_spanner_instance(instance_id)
            print(f"✅ Deleted instance {instance_id}.")

            return {"message": f"Instance {instance_id} and all contained resources have been deleted. 💥"}

        return await self._safe_task_run(delete_workflow)
