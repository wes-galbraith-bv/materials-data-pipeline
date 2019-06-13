import pandas as pd

from config import Config
from db import DatabaseManager
from csv_reader import CSVReader, Target


class Pipeline:

    def extract(self):
        csv_reader = CSVReader(Config.directory)
        csv_reader.targets = [Target(*params) for params in Config.targets]
        received, shipped, history, picked = csv_reader.read()
        return received, shipped, history, picked

    def clean(self, received, shipped, history, picked):
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
        picked = picked.astype({'AuthNo': 'object'})
        picked_grouped = picked.groupby(by=['ProjectSiteId', 'AuthNo', 'ItemNo'])
        picked_agg = picked_grouped.sum().reset_index()
        return picked_agg

    def merge_history_picked(self, history_agg, picked_agg):
        materials = history_agg.merge(picked_agg, how='left', on=['ProjectSiteId', 'AuthNo'])
        materials = materials.drop('level_2', axis=1)
        return materials

    def agg_shipped(self, shipped):
        shipped_grouped = shipped.groupby(by=['AuthNo', 'ProjectSiteId', 'ItemNo'])
        shipped_agg = shipped_grouped.agg({'DeployedDate': 'max', 'DeployedQuantity': 'sum'})
        shipped_agg = shipped_agg.reset_index()
        return shipped_agg

    def merge_materials_shipped(self, materials, shipped):
        materials = materials.copy()
        shipped = shipped.copy()
        materials = materials.astype({'AuthNo': 'object'})
        shipped = shipped.astype({'AuthNo': 'object'})
        assert materials.dtypes['AuthNo'] == 'object'
        assert shipped.dtypes['AuthNo'] == 'object'
        assert ((materials.ProjectSiteId.isin(shipped.ProjectSiteId)) & (materials.AuthNo.isin(shipped.AuthNo)) & (materials.ItemNo.isin(shipped.ItemNo))).sum() == 0
        materials = materials.merge(shipped, how='left', on=['AuthNo', 'ProjectSiteId'])
        materials['ItemNo'] = materials.ItemNo_y.fillna(materials.ItemNo_x)
        materials = materials.reset_index()
        materials = materials.drop(['index', 'ItemNo_x', 'ItemNo_y'], axis=1)
        materials = materials.sort_values(['ProjectSiteId', 'AuthNo', 'ItemNo'])
        return materials

    def agg_received(self, received):
        def sum_qty_most_recent_everything_else(df):
            received_qty = df.ReceivedQuantity.sum()
            df = df.sort_values('ReceivedDate', ascending=False).head(1)
            df['ReceivedQuantity'] = received_qty
            return df
        grouped = received.groupby(by=['ProjectSiteId','ItemNo', 'PONo'])
        received_agg = grouped.apply(sum_qty_most_recent_everything_else)
        return received_agg

    def transform(self, received, shipped, history, picked):
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
        db = DatabaseManager(Config.driver, Config.username, Config.password,
                             Config.server, Config.database, Config.schema)
        for df in dfs:
            db.insert_df(df)

    def run(self):
        dfs = self.extract()
        dfs = self.clean(*dfs)
        dfs = self.transform(*dfs)
        self.load(*dfs)
        return dfs


class MergePipeline(Pipeline):

    def transform(self, received, shipped, history, picked):
        history_agg = self.agg_history(history)
        picked_agg = self.agg_picked(picked)
        materials = self.merge_history_picked(history_agg, picked_agg)
        shipped_agg = self.agg_shipped(shipped)
        materials = self.merge_materials_shipped(materials, shipped)
        received = self.agg_received(received)
        received.name = 'InventoryReceived'
        materials.name = 'InventoryStagedDeployed'
        return received, materials
