Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Dynamo.Providers
Imports System.Configuration
Imports Dynamo.Expressions
Imports Dynamo.Entities

<TestClass()>
Public Class ArangoDBTest

    Private Repository As ArangoDBProvider.Repository
    Private Query As ArangoDBProvider.QueryBuilder
    Private TestEntityID As String
    Private TestEntities As List(Of Entity)

    <TestMethod>
    Public Sub CreateRepository()
        Dim ConnectionString = ConfigurationManager.ConnectionStrings("ArangoDB").ConnectionString
        Assert.IsFalse(String.IsNullOrWhiteSpace(ConnectionString))

        Repository = New ArangoDBProvider.Repository(ConnectionString)
        Assert.IsNotNull(Repository)

    End Sub

    Public Sub FirstQuery()
        If Repository Is Nothing Then CreateRepository()
        Query = Repository.Query("FirstTable", "ft")
        Assert.IsNotNull(Query)
        Assert.AreEqual(Query.Entities.FirstOrDefault.Value, "FirstTable")
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
        Query.WithNested("SecondTable", "st", "ft").By("ParentID", "_key")
        Dim Result = Query.Execute
        Dim NestedResult As List(Of Entity) = Result.Where(Function(w) w.Id = 1).FirstOrDefault.Fields("st")
        Assert.AreEqual(NestedResult.Count, 2)
        Assert.AreEqual(NestedResult.Where(Function(s) s.Fields("IntegerNumber") > 100).Count, 0)

        'CASE 2 - Three levels of nested object with filter on first level and two different entities in second level
        FirstQuery()
        Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2")
        Query.WithNested("SecondTable", "ST", "ft").By("ParentID", "_key")
        Query.WithNested("ThirdTable", "Tt", "ft").By("ParentID", "_key")
        Query.WithNested("FourthTable", "fht", "st").By("ParentID", "_key")
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 2)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Name"), "Test2")
        'Dim FTLevel = Result.Where(Function(w) w.Id = New Guid("C38BE8E1-440E-4DB8-A235-A35C428913BF")).FirstOrDefault
        Dim FTLevel = Result.Where(Function(w) w.Id = 2).FirstOrDefault
        Assert.AreEqual(FTLevel.Fields("ST").Count, 1)
        Assert.AreEqual(FTLevel.Fields("Tt").Count, 2)
        Dim STLevel = DirectCast(FTLevel.Fields("ST"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(STLevel.Fields("fht").Count, 2)
        Dim FHTLevel = DirectCast(STLevel.Fields("fht"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(FHTLevel.Name.StartsWith("TEST-C"), True)

        'CASE 3 - Three levels of nested object with filter on second level and two different entities in second level
        FirstQuery()
        Query.FilterBy("ST", "Name", FilterOperators.Equal, "TEST-C")
        Query.WithNested("SecondTable", "ST", "ft").By("ParentID", "_key")
        Query.WithNested("ThirdTable", "Tt", "ft").By("ParentID", "_key")
        Query.WithNested("FourthTable", "fht", "ST").By("ParentID", "_key")
        Result = Query.Execute
        Assert.AreEqual(Result.Count, 1)
        'Assert.AreEqual(Result.FirstOrDefault.Fields("ID"), New Guid("C38BE8E1-440E-4DB8-A235-A35C428913BF"))
        Assert.AreEqual(Result.FirstOrDefault.Fields("_key"), "2")
        Assert.AreEqual(Result.FirstOrDefault.Fields("ST").Count, 1)
        Assert.AreEqual(Result.FirstOrDefault.Fields("Tt").Count, 2)
        STLevel = DirectCast(Result.FirstOrDefault.Fields("ST"), List(Of Entity)).FirstOrDefault
        Assert.AreEqual(CType(STLevel.Fields("DecimalNumber"), Decimal), 99D)
        Dim TTLevel = (From i As Entity In DirectCast(Result.FirstOrDefault.Fields("Tt"), List(Of Entity)) Where i.Name = "MyTest3_3" Select i).FirstOrDefault
        Assert.AreEqual(TTLevel.Fields("NullableBoolean"), True)
        Assert.AreEqual(STLevel.Fields("fht").count, 2)

        'CASE 4 - Three levels of nested object without conditions (FULL TREE)
        FirstQuery()
        Query.WithNested("SecondTable", "ST", "ft").By("ParentID", "_key")
        Query.WithNested("ThirdTable", "Tt", "ft").By("ParentID", "_key")
        Query.WithNested("FourthTable", "fht", "ST").By("ParentID", "_key")
        Result = Query.Execute
        Dim STLevels As List(Of Entity) = Result.Where(Function(w) w.Id = "3").FirstOrDefault.Fields("ST")
        Assert.AreEqual(STLevels.Count, 0)
        STLevels = Result.Where(Function(w) w.Id = "1").FirstOrDefault.Fields("ST")
        Assert.AreEqual(STLevels.Count, 2)
        Dim FHTLevels As List(Of Entity) = STLevels.Where(Function(w) w.Id = "1").FirstOrDefault.Fields("fht")
        Assert.AreEqual(FHTLevels.Count, 1)
        Dim TTLevels As List(Of Entity) = Result.Where(Function(w) w.Id = "2").FirstOrDefault.Fields("Tt")
        Assert.AreEqual(TTLevels.Count, 2)


    End Sub

    <TestMethod>
    Public Sub DynamicObject()
        FirstQuery()
        Query.WithNested("SecondTable", "ST", "ft").By("ParentID", "_key")
        Query.WithNested("ThirdTable", "Tt", "ft").By("ParentID", "_key")
        Query.WithNested("FourthTable", "fht", "ST").By("ParentID", "_key")
        Dim Result = Query.Execute
        Dim FirstEntity As Object = Result(1)
        Assert.AreEqual(FirstEntity.Name, "Test2")
        Assert.AreEqual(FirstEntity.Test, 3L)
        Assert.AreEqual(FirstEntity.ST.Count, 2)
    End Sub

    <TestMethod>
    Public Sub CreateEntity()
        If Repository Is Nothing Then CreateRepository()
        Dim TestEntity As New Entity("FirstTable")
        TestEntity.Fields.Add("Name", "INSERT TEST 1")
        TestEntity.Fields.Add("Test", 4)
        Assert.IsNull(TestEntity.Id)
        Assert.IsNotNull(TestEntity.Schema.EntityName)
        Assert.AreEqual(TestEntity.Schema.EntityName, "FirstTable")
        Repository.AddEntity(TestEntity)
        Assert.IsNotNull(TestEntity.Id)
        Assert.AreEqual(TestEntity.Fields("Test"), 4)
        Assert.AreEqual(TestEntity.Fields("Name"), "INSERT TEST 1")
        TestEntityID = TestEntity.Id
    End Sub

    <TestMethod()>
    Public Sub CreateEntities()
        If Repository Is Nothing Then CreateRepository()
        TestEntities = New List(Of Entity)
        Dim TestEntity As New Entity("FirstTable")
        TestEntity.Fields.Add("Name", "INSERT TEST MULTIPLE 2-1")
        TestEntity.Fields.Add("Test", 5)
        TestEntities.Add(TestEntity)
        TestEntity = New Entity("FirstTable")
        TestEntity.Fields.Add("Name", "INSERT TEST MULTIPLE 2-2")
        TestEntity.Fields.Add("Test", 5)
        TestEntities.Add(TestEntity)
        Repository.AddEntities(TestEntities)
        Assert.IsNotNull(TestEntities.FirstOrDefault.Name)
        Assert.AreEqual(TestEntities(1).Name, "INSERT TEST MULTIPLE 2-2")

    End Sub

    <TestMethod>
    Public Sub UpdateEntity()
        'Partial Update
        If Repository Is Nothing Then CreateRepository()
        CreateEntity()
        Dim TestEntity As New Entity("FirstTable")
        TestEntity.Id = TestEntityID
        TestEntity.Fields("Name") = "INSERT TEST 1 - Partial Update"
        Repository.UpdateEntity(TestEntity)
        Dim FirstTestResult = Repository.Query("FirstTable", "FT").FilterBy("FT", "_key", FilterOperators.Equal, TestEntityID).Execute.FirstOrDefault
        Assert.AreEqual(FirstTestResult.Name, "INSERT TEST 1 - Partial Update")

        'Entire EntityUpdate
        FirstTestResult.Fields("Name") = "INSERT TEST 1 - Complete Update"
        FirstTestResult.Fields("Test") = 44
        Repository.UpdateEntity(FirstTestResult)
        Dim SecondTestResult = Repository.Query("FirstTable", "FT").FilterBy("FT", "_key", FilterOperators.Equal, FirstTestResult.Id).Execute.FirstOrDefault
        Assert.AreEqual(SecondTestResult.Fields("Test"), 44L)

    End Sub

    <TestMethod>
    Public Sub UpdateEntities()
        If Repository Is Nothing Then CreateRepository()
        CreateEntities()
        For Each Entity In TestEntities
            Entity.Fields("Name") = Entity.Fields("Name") + " Update 3"
            Entity.Fields("Test") = Entity.Fields("Test") + 100
        Next
        Repository.UpdateEntities(TestEntities)
        Dim TestResult = Repository.Query("FirstTable", "FT").FilterBy("FT", "_key", FilterOperators.Equal, TestEntities.FirstOrDefault.Id).Execute.FirstOrDefault
        Assert.AreEqual(TestResult.Fields("Test") > 100L, True)
        Assert.AreEqual(TestResult.Fields("Name").EndsWith("Update 3"), True)

    End Sub
End Class
