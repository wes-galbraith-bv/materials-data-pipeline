from sqlalchemy.types import Integer, String, DateTime

class Config:
    driver = "SQL Server Native Client 11.0"
    username = "GEOMRE"
    password = "Passw!@#"
    server = "199.73.50.120\\CUS7602TP3,48000"
    database = "V3_Central"
    schema = "GeoMRE"
    directory = "//na/data/Corp/finance2/private/__Telecom Division/Tellworks/Reports/AIMS Reports"
    targets = (
        ('Received', r'^ReceivedData_.*\.csv$', 'InventoryReceived',
         {
            'DatRcvd': 'ReceivedDate',
            'PartNum': 'ItemNo',
            'Description': 'ItemDescription',
            'Market': 'WarehouseLocation',
            'Qty': 'ReceivedQuantity',
            'PACENum': 'ProjectSiteId',
            'OrderNum': 'PONo',
            'Condition': 'InventoryType'
         }),
        ('Shipped', r'^ShippedData_.*\.csv$', 'InventoryShipped',
         {
            'Auth#': 'AuthNo',
            'PACENum': 'ProjectSiteId',
            'PartNum': 'ItemNo',
            'OrderNum': 'PONo',
            'Qty': 'DeployedQuantity',
            'ShipDate': 'DeployedDate'
         }),
        ('Site Equipment Deployment History', r'^SiteEquipDepData_.*\.csv$', 'SiteEquipmentDeploymentHistory',
         {
            'Pickup Auth #': 'AuthNo',
            'Status': 'FulfillmentStatus',
            'Staged Date': 'StagedDate',
            'PACENum': 'ProjectSiteId',
            'Pickup Company': 'Subcontractor'
         }),
        ('Staged Inventory Details', r'^InventoryPickData_.*\.csv$', 'InventoryPicked',
         {
            'PartNum': 'ItemNo',
            'Auth Number': 'AuthNo',
            'Picked QTY': 'StagedQuantity',
            'PACENum': 'ProjectSiteId'
         })
    )
    dtypes = {
        'InventoryReceived': {
            'ReceivedDate': DateTime(),
            'ItemNo': String(100),
            'ItemDescription': String(1000),
            'WarehouseLocation': String(500),
            'ReceivedQuantity': Integer(),
            'ProjectSiteId': String(100),
            'PONo': String(100),
            'InventoryType': String(100)
        },
        'InventoryStagedDeployed': {
            'AuthNo': String(100),
            'ProjectSiteId': String(100),
            'FulfillmentStatus': String(100),
            'StagedDate': DateTime(),
            'Subcontractor': String(200),
            'StagedQuantity': Integer(),
            'DeployedDate': DateTime(),
            'DeployedQuantity': Integer(),
            'PONo': String(100),
            'ItemNo': String(100)
        }
    }
