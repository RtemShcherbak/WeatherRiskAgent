import logging
import aiohttp
import asyncio
from pathlib import Path
from datetime import date

from weather_risk_agent.app.configs import Constant
from weather_risk_agent.app.ingestion.loaders.base_loader import BaseLoader, LoadTask
from weather_risk_agent.app.ingestion.sources import GEFSSource



class GEFSLoader(BaseLoader):
    def __init__(
            self, 
            raw_dir, 
            source = "gefs", 
            overwrite = False,
        ):
        super().__init__(source, raw_dir, overwrite)
        self.const = Constant()
        self.num_members = 30


    def _generate_tasks(
            self, 
            run_date, 
            lead_days, 
            cycles,
            var: str = "TMP:2 m above ground", 
    ):
        file_descriptions = []
        for lead_day in range(lead_days):
            for cycle in cycles:
                fxx = [(i + (lead_day+1)*24 - cycle) for i in range(0, 24, 24//len(cycles))]
                for fxx_ in fxx:
                    for member in range(0, self.num_members, 1):
                        file_desc = {
                            'run_date': run_date,
                            'cycle_hh': cycle,
                            'member': member,
                            'fxx': fxx_,
                            'product': "atmos.25",
                        }
                        file_descriptions.append(file_desc)
        
        source = self.source.select_source(**file_descriptions[0])
        source_file = self.source.get_source_file(source, **file_descriptions[0])
        inv = self.source.inventory(source_file.source, source_file.path)
        search = self.source.search(var, inv)

        tasks = []
        for file_desc in file_descriptions:
            source_file = self.source.get_source_file(source, **file_desc)
            tasks.append(
                LoadTask(
                    url=source_file.url,
                    source=source_file.source,
                    path=source_file.path,
                    run_date=source_file.run_date,
                    fxx=source_file.fxx,
                    product=source_file.product,
                    member=source_file.member,

                    data_type="forecast",
                    variable=var,
                    start_byte=search['start_byte'].values[0],
                    end_byte=search['end_byte'].values[0],
                )
            )

        return tasks
    


    async def _download_one(self, session, load_task):

        return await super()._download_one(session, load_task)
    

    # async def _adownload_all(
    #         self,
    #         var: str,
    #         run_date: date,
    #         lead_days: int,
    #         cycles: tuple[int] = [0, 6, 12, 18],
    # ) -> dict[str, list]:
    #     ## pre-creat tasks
    #     for lead_day in range(lead_days):
    #         fxx = [(i + (lead_day+1)*24 - cycle) for i in range(0, 24, 6)]
    #         for fxx_ in fxx:
    #             for member in range(0, 30, 1):
    #                 file_desc = {
    #                     'run_date': date(2026, 3, 25),
    #                     'cycle_hh': cycle,
    #                     'member': member,
    #                     'fxx': fxx_,
    #                     'product': "atmos.25",
    #                 }
    #                 file_descriptions.append(file_desc)
        
    #     async with aiohttp.ClientSession() as session:



            
    #         load_task = await self._download_one(
    #             session=session,
    #             load_task=load_task,
    #         )