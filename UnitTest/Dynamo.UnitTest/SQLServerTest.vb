Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Dynamo.Providers
Imports System.Configuration
Imports Dynamo.Expressions
Imports Dynamo.Entities

<TestClass()>
Public Class SQLServerTest

    '*** SQL SERVER ***
    'Private Repository As SQLServerProvider.SQLRepository
    'Private Query As SQLServerProvider.SQLQueryBuilder

    '*** NuoDB SERVER ***
    Private Repository As NuoDBProvider.Repository
    Private Query As NuoDBProvider.QueryBuilder

    <TestMethod>
    Public Sub CreateRepository()
        Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
        Assert.IsFalse(String.IsNullOrWhiteSpace(ConnectionString))

        Repository = New NuoDBProvider.Repository(ConnectionString)
        Assert.IsNotNull(Repository)

    End Sub

    Public Sub FirstQuery()
        If Repository Is Nothing Then CreateRepository()
        Query = Repository.Query("firsttable", "ft")
        Assert.IsNotNull(Query)
        'Assert.AreEqual(Query.Entities.FirstOrDefault.Value, "firsttable")
        'Assert.AreEqual(Query.Entities.FirstOrDefault.Key, "ft")
    End Sub

    <TestMethod>
    Public Sub GetAll()
        If Query Is Nothing Then FirstQuery()

        Dim Result = Query.Execute()
        Assert.AreEqual(Result.Count, 4)
    End Sub

    <TestMethod()>
    Public Sub FilterBy()
        FirstQuery()
        Query.FilterBy("ft", "test", FilterOperators.Include + FilterOperators.Not, New String() {1, 3})
        Dim Result = Query.Execute
        Assert.AreEqual(Result.Count, 1)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Test"), 2)

        FirstQuery()
        Query.FilterBy(New DynamoFilterExpression("ft", "test", FilterOperators.GreaterThan, 2))
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 1)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Name"), "Test2")

        FirstQuery()
        Query.FilterBy(New DynamoFilterExpression("ft", "Name", FilterOperators.Equal, "Test2"))
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 2)
        Assert.AreNotEqual(Result.FirstOrDefault.Fields("Test"), 2)

        Query.OrFilterBy("ft", "Test", FilterOperators.LessThan, 2)
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 3)
        Assert.AreNotEqual(Result.FirstOrDefault.Fields("Test"), 2)

        Query.OrFilterBy(New DynamoFilterExpression("ft", "Test", FilterOperators.Equal + FilterOperators.GreaterThan, 2))
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 4)

        FirstQuery()
        Query.FilterBy(New DynamoFilterExpression("ft", "Name", FilterOperators.Pattern, "%1"))
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 2)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Name"), "Test1")

        Query.FilterBy(New List(Of DynamoFilterExpression) From {New DynamoFilterExpression("ft", "Test", FilterOperators.GreaterThan + FilterOperators.Equal, -1),
                                                                 New DynamoFilterExpression("ft", "Test", FilterOperators.Equal + FilterOperators.LessThan, 1)}, FilterCombiners.And)
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 1)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Test"), 1)

        FirstQuery()
        Query.FilterBy(New DynamoFilterExpression("ft", "Name", FilterOperators.Pattern, "%pippo%"))
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 0)

        Query.OrFilterBy(New List(Of DynamoFilterExpression) From {New DynamoFilterExpression("ft", "Test", FilterOperators.Equal, 2),
                                                                 New DynamoFilterExpression("ft", "Test", FilterOperators.Equal, 3)}, FilterCombiners.Or)
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 2)

    End Sub

    <TestMethod>
    Public Sub SortBy()
        FirstQuery()
        Query.SortBy("ft", "test", SortDirections.Ascending)
        Assert.AreEqual(Query.Execute().FirstOrDefault.Fields("Test"), 1)

        FirstQuery()
        Query.SortBy("ft", "test", SortDirections.Descending)
        Assert.AreEqual(Query.Execute().FirstOrDefault.Fields("Test"), 3)

    End Sub

    '<TestMethod>
    'Public Sub Join()

    '    ''TEST 1: 1-N Two levels, One child type
    '    'FirstQuery()
    '    'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
    '    '    .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
    '    'Dim Result = Query.Execute
    '    'Assert.AreEqual(2, Result.Count)
    '    'Dim Test1 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "23aee672-db40-499e-95fc-4cdb7897187e" Select i).FirstOrDefault()
    '    'Assert.IsNotNull(Test1)
    '    'Assert.AreEqual(Test1.Fields("SecondTable").Count, 2)
    '    'Assert.IsTrue(DirectCast(Test1.Fields("SecondTable"), List(Of Entity)).FirstOrDefault.Fields.ContainsKey("AData"))

    '    ''TEST 2: 1-N Two levels, Two child type
    '    'FirstQuery()
    '    'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
    '    '     .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID") _
    '    '     .Join("Thirdtable", "tt", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
    '    'Result = Query.Execute
    '    'Dim Test2_1 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "23aee672-db40-499e-95fc-4cdb7897187e" Select i).FirstOrDefault()
    '    'Assert.AreEqual(Test2_1.Fields("SecondTable").Count, 2)
    '    'Assert.AreEqual(Test2_1.Fields("Thirdtable").Count, 1)
    '    'Dim Test2_2 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "c38be8e1-440e-4db8-a235-a35c428913bf" Select i).FirstOrDefault()
    '    'Assert.AreEqual(Test2_2.Fields("SecondTable").Count, 1)
    '    'Assert.AreEqual(Test2_2.Fields("Thirdtable").Count, 2)
    '    'Assert.IsTrue(DirectCast(Test2_2.Fields("Thirdtable"), List(Of Entity)).FirstOrDefault.Fields("Name").ToString().StartsWith("MyTest3_"))
    '    'Assert.AreEqual(DirectCast(Test2_1.Fields("Thirdtable"), List(Of Entity)).FirstOrDefault.Fields("NullableInt"), 123)

    '    ''TEST 3: 1-N Three levels, One child type per level
    '    'FirstQuery()
    '    'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
    '    '     .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID") _
    '    '     .Join("FOURTHTABLE", "frt", True).By("ParentID", RelationshipOperators.Equal, "st", "ID")
    '    'Result = Query.Execute



    '    'TEST 4
    '    FirstQuery()
    '    Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
    '    .Join("SecondTable", "st", True, NestedEntityType.MultipleEntity).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
    '    Dim Result = Query.Execute




    'End Sub

    <TestMethod>
    Public Sub WithNested()

        'CASE 1 - Two levels of nested object
        FirstQuery()
        Query.WithNested("SecondTable", "st", "ft").By("parentid", "id")
        Dim Result = Query.Execute
        Assert.AreEqual(Result.Count, 4)
        'Dim NestedResult As List(Of Entity) = Result.Where(Function(w) w.Id = New Guid("23AEE672-DB40-499E-95FC-4CDB7897187E")).FirstOrDefault.Fields("st")
        Dim NestedResult As List(Of Entity) = Result.Where(Function(w) w.Id = 1).FirstOrDefault.Fields("st")
        Assert.AreEqual(NestedResult.Count, 2)
        Assert.AreEqual(NestedResult.Where(Function(s) s.Fields("IntegerNumber") > 100).Count, 0)

        'CASE 2 - Three levels of nested object with filter on first level and two different entities in second level
        FirstQuery()
        Query.FilterBy("ft", "name", FilterOperators.Equal, "Test2")
        Query.WithNested("Secondtable", "ST", "ft").By("parentID", "Id")
        Query.WithNested("ThirdTABLE", "Tt", "ft").By("parentid", "ID")
        Query.WithNested("FourthTable", "fht", "st").By("parentid", "id")
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 2)
        Assert.AreEqual(Result.FirstOrDefault.Fields("nAme"), "Test2")
        'Dim FTLevel = Result.Where(Function(w) w.Id = New Guid("C38BE8E1-440E-4DB8-A235-A35C428913BF")).FirstOrDefault
        Dim FTLevel = Result.Where(Function(w) w.Id = 2).FirstOrDefault
        Assert.AreEqual(FTLevel.Fields("st").Count, 1)
        Assert.AreEqual(FTLevel.Fields("tt").Count, 2)
        Dim STLevel = DirectCast(FTLevel.Fields("st"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(STLevel.Fields("FHT").Count, 2)
        Dim FHTLevel = DirectCast(STLevel.Fields("FHT"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(FHTLevel.Name.StartsWith("TEST-C"), True)

        'CASE 3 - Three levels of nested object with filter on second level and two different entities in second level
        FirstQuery()
        Query.FilterBy("st", "name", FilterOperators.Equal, "TEST-C")
        Query.WithNested("Secondtable", "ST", "ft").By("parentID", "Id")
        Query.WithNested("ThirdTABLE", "Tt", "ft").By("parentid", "ID")
        Query.WithNested("FourthTable", "fht", "st").By("parentid", "id")
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 1)
        'Assert.AreEqual(Result.FirstOrDefault.Fields("ID"), New Guid("C38BE8E1-440E-4DB8-A235-A35C428913BF"))
        Assert.AreEqual(Result.FirstOrDefault.Fields("ID"), 2)
        Assert.AreEqual(Result.FirstOrDefault.Fields("st").Count, 1)
        Assert.AreEqual(Result.FirstOrDefault.Fields("tt").Count, 2)
        STLevel = DirectCast(Result.FirstOrDefault.Fields("ST"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(STLevel.Fields("decimalnumber"), 99D)
        Dim TTLevel = (From i As Entity In DirectCast(Result.FirstOrDefault.Fields("tT"), List(Of Entity)) Where i.Name = "MyTest3_3" Select i).FirstOrDefault
        Assert.AreEqual(TTLevel.Fields("NullableBoolean"), True)
        Assert.AreEqual(STLevel.Fields("FHT").count, 2)

        'CASE 4 - Three levels of nested object without conditions (FULL TREE)
        FirstQuery()
        Query.WithNested("Secondtable", "ST", "ft").By("parentID", "Id")
        Query.WithNested("ThirdTABLE", "Tt", "ft").By("parentid", "ID")
        Query.WithNested("FourthTable", "fht", "st").By("parentid", "id")
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 4)
        Dim STLevels As List(Of Entity) = Result.Where(Function(w) w.Id = 3).FirstOrDefault.Fields("st")
        Assert.AreEqual(STLevels.Count, 0)
        STLevels = Result.Where(Function(w) w.Id = 1).FirstOrDefault.Fields("st")
        Assert.AreEqual(STLevels.Count, 2)
        Dim FHTLevels As List(Of Entity) = STLevels.Where(Function(w) w.Id = 2).FirstOrDefault.Fields("fht")
        Assert.AreEqual(FHTLevels.Count, 1)
        Dim TTLevels As List(Of Entity) = Result.Where(Function(w) w.Id = 2).FirstOrDefault.Fields("tt")
        Assert.AreEqual(TTLevels.Count, 2)


    End Sub
End Class
