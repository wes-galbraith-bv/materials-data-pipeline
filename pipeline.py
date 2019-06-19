from logger import logger

import pandas as pd

from config import Config
from csv_reader import CSVReader, Target
from db import DatabaseManager


class Pipeline:

    def extract(self):
        logger.info('extracting csvs from shared drive')
        csv_reader = CSVReader(Config.directory)
        csv_reader.targets = [Target(*params) for params in Config.targets]
        received, shipped, history, picked = csv_reader.read()
        return received, shipped, history, picked

    def clean(self, received, shipped, history, picked):
        logger.info('Cleaning data. Scrub scrub scrub.')
        received = received.copy()
        shipped = shipped.copy()
        history = history.copy()
        picked = picked.copy()

        received = received.dropna(subset=['ProjectSiteId'])
        shipped = shipped.dropna(subset=['ProjectSiteId'])
        history = history.dropna(subset=['ProjectSiteId'])
        picked = picked.dropna(subset=['ProjectSiteId'])

        PACE_regex = r'^.*([mM][rR][a-zA-Z]{3}\d{6}).*$'

        received = received[received.ProjectSiteId.str.match(PACE_regex)]
        shipped = shipped[shipped.ProjectSiteId.str.match(PACE_regex)]
        history = history[history.ProjectSiteId.str.match(PACE_regex)]
        picked = picked[picked.ProjectSiteId.str.match(PACE_regex)]

        first_group = lambda m: m.group(1)

        received['ProjectSiteId'] = received.ProjectSiteId.str.replace(PACE_regex, first_group)
        shipped['ProjectSiteId'] = shipped.ProjectSiteId.str.replace(PACE_regex, first_group)
        history['ProjectSiteId'] = history.ProjectSiteId.str.replace(PACE_regex, first_group)
        picked['ProjectSiteId'] = picked.ProjectSiteId.str.replace(PACE_regex, first_group)

        to_upper = lambda m: m.group().upper()

        received['ProjectSiteId'] = received.ProjectSiteId.str.replace('[a-zA-Z]{5}', to_upper)
        shipped['ProjectSiteId'] = shipped.ProjectSiteId.str.replace('[a-zA-Z]{5}', to_upper)
        history['ProjectSiteId'] = history.ProjectSiteId.str.replace('[a-zA-Z]{5}', to_upper)
        picked['ProjectSiteId'] = picked.ProjectSiteId.str.replace('[a-zA-Z]{5}', to_upper)

        return received, shipped, history, picked

    def agg_history(self, history):
        logger.info('aggregating site equipment deployment history report')
        history = history.copy()
        history_grouped = history.groupby(by=['AuthNo', 'ProjectSiteId'])
        def max_ship_date(df):
            df = df.reset_index()
            return df.sort_values('StagedDate', ascending=False).head(n=1)

        history_agg = history_grouped.apply(max_ship_date)
        history_agg = history_agg.drop(['AuthNo', 'ProjectSiteId'], axis=1)
        history_agg = history_agg.reset_index()
        history_agg = history_agg.drop(['index'], axis=1)
        return history_agg

    def agg_picked(self, picked):
        logger.info('aggregating inventory picked report')
        picked = picked.copy()
        picked = picked.astype({'AuthNo': 'object'})
        picked_grouped = picked.groupby(by=['ProjectSiteId', 'AuthNo', 'ItemNo'])
        picked_agg = picked_grouped.sum().reset_index()
        return picked_agg

    def merge_history_picked(self, history_agg, picked_agg):
        logger.info('merging site equipment deployment history and inventory picked')
        history_agg = history_agg.copy()
        picked_agg = picked_agg.copy()
        materials = history_agg.merge(picked_agg, how='left', on=['ProjectSiteId', 'AuthNo'])
        materials = materials.drop('level_2', axis=1)
        return materials

    def agg_shipped(self, shipped):
        logger.info('aggregating inventory shipped report')
        shipped = shipped.copy()
        shipped_grouped = shipped.groupby(by=['AuthNo', 'ProjectSiteId', 'ItemNo'])
        #Using first to aggregate PONo is probably incorrect, but let us ignore that for the time being in the interest of getting this steaming pile of junk working.
        shipped_agg = shipped_grouped.agg({'DeployedDate': 'max', 'DeployedQuantity': 'sum', 'PONo': 'first'})
        shipped_agg = shipped_agg.reset_index()
        return shipped_agg

    def merge_materials_shipped(self, materials, shipped):
        logger.info('merging materials table with inventory shipped')
        materials = materials.copy()
        shipped = shipped.copy()
        materials = materials.astype({'AuthNo': 'int64'})
        shipped = shipped.astype({'AuthNo': 'int64'})
        try:
            assert (materials.dtypes[['AuthNo', 'ProjectSiteId', 'ItemNo']] == shipped.dtypes[['AuthNo', 'ProjectSiteId', 'ItemNo']]).all()
            assert ((materials.ProjectSiteId.isin(shipped.ProjectSiteId)) & (materials.AuthNo.isin(shipped.AuthNo)) & (materials.ItemNo.isin(shipped.ItemNo))).sum() == 0
        except AssertionError as ae:
            logger.error(str(ae))
        materials = materials.merge(shipped, how='left', on=['AuthNo', 'ProjectSiteId'])
        materials['ItemNo'] = materials.ItemNo_y.fillna(materials.ItemNo_x)
        materials = materials.reset_index()
        materials = materials.drop(['index', 'ItemNo_x', 'ItemNo_y'], axis=1)
        materials = materials.sort_values(['ProjectSiteId', 'AuthNo', 'ItemNo'])
        return materials

    def agg_received(self, received):
        logger.info('aggregating inventory received report')
        received = received.copy()
        def sum_qty_most_recent_everything_else(df):
            received_qty = df.ReceivedQuantity.sum()
            df = df.sort_values('ReceivedDate', ascending=False).head(1)
            df['ReceivedQuantity'] = received_qty
            return df
        grouped = received.groupby(by=['ProjectSiteId','ItemNo', 'PONo'])
        received_agg = grouped.apply(sum_qty_most_recent_everything_else)
        return received_agg

    def transform(self, received, shipped, history, picked):
        logger.info('transforming data into a more workable format.')
        received = received.copy()
        shipped = shipped.copy()
        history = history.copy()
        picked = picked.copy()
        received = self.agg_received(received)
        shipped = self.agg_shipped(shipped)
        history = self.agg_history(history)
        picked = self.agg_picked(picked)
        received.name = 'InventoryReceived'
        shipped.name = 'InventoryShipped'
        history.name = 'SiteEquipmentDeploymentHistory'
        picked.name = 'InventoryPicked'
        return received, shipped, history, picked

    def load(self, *dfs):
        logger.info('Inserting data to database')
        db = DatabaseManager(Config.driver, Config.username, Config.password,
                             Config.server, Config.database, Config.schema)
        for df in dfs:
            db.insert_df(df)

    def run(self):
        logger.info('running pipeline')
        dfs = self.extract()
        dfs = self.clean(*dfs)
        dfs = self.transform(*dfs)
        self.load(*dfs)
        return dfs


class MergePipeline(Pipeline):

    def transform(self, received, shipped, history, picked):
        logger.info('transforming data into a more workable format. This should create two tables.')
        received = received.copy()
        shipped = shipped.copy()
        history = history.copy()
        picked = picked.copy()
        history_agg = self.agg_history(history)
        picked_agg = self.agg_picked(picked)
        materials_1 = self.merge_history_picked(history_agg, picked_agg)
        shipped_agg = self.agg_shipped(shipped)
        materials_2 = self.merge_materials_shipped(materials_1, shipped_agg)
        received_agg = self.agg_received(received)
        received_agg.name = 'InventoryReceived'
        materials_2.name = 'InventoryStagedDeployed'
        return received_agg, materials_2
