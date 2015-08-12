Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Dynamo.Providers
Imports System.Configuration
Imports Dynamo.Expressions
Imports Dynamo.Entities

<TestClass()>
Public Class SQLiteTest

    Private Repository As SQLiteProvider.Repository
    Private Query As SQLiteProvider.QueryBuilder

    <TestMethod>
    Public Sub CreateRepository()
        Dim ConnectionString = ConfigurationManager.ConnectionStrings("SQLite").ConnectionString
        Assert.IsFalse(String.IsNullOrWhiteSpace(ConnectionString))

        Repository = New SQLiteProvider.Repository(ConnectionString)
        Assert.IsNotNull(Repository)

    End Sub

    Public Sub FirstQuery()
        If Repository Is Nothing Then CreateRepository()
        Query = Repository.Query("firsttable", "ft")
        Assert.IsNotNull(Query)
        Assert.AreEqual(Query.Entities.FirstOrDefault.Value, "firsttable")
        Assert.AreEqual(Query.Entities.FirstOrDefault.Key, "ft")
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
