class Config:
    driver = "SQL Server Native Client 11.0"
    username = "GEOMRE"
    password = "P@ssw!@#"
    server = "LSPR02-0402-87\\CUS7602TPTEST,48001"
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
    logfile = 'C:\\Users\\gal98383\\projects\\materials_api\\logs\\info.log'
