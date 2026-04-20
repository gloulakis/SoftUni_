[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimDate csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"DimDate csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, PreserveOrder = null]),
  #"Navigation 1" = Source{[workspaceId = "6d1c6fc6-5ac4-4223-96e9-e85e5dd2d40c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "5e077f10-283d-4f4e-ab64-81f43914cdd7"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "DimDate.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"DateKey", Int64.Type}, {"Date", type date}, {"Year", Int64.Type}, {"Quarter", type text}, {"MonthNo", Int64.Type}, {"MonthName", type text}, {"YearMonth", type date}, {"WeekNo", Int64.Type}, {"Day", Int64.Type}, {"DayName", type text}})
in
  #"Changed column type";
shared #"DimDate csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, PreserveOrder = null]),
  Navigation_1 = Pattern{[workspaceId = "6d1c6fc6-5ac4-4223-96e9-e85e5dd2d40c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5e077f10-283d-4f4e-ab64-81f43914cdd7"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim Date", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
