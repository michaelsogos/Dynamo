Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Dynamo.Providers
Imports System.Configuration
Imports Dynamo.Expressions
Imports Dynamo.Entities

<TestClass()>
Public Class SQLServerTest

    Private Repository As SQLRepository
    Private Query As SQLQueryBuilder

    <TestMethod>
    Public Sub CreateRepository()
        Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
        Assert.IsFalse(String.IsNullOrWhiteSpace(ConnectionString))

        Repository = New SQLRepository(ConnectionString)
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
    Public Sub Join()

        ''TEST 1: 1-N Two levels, One child type
        'FirstQuery()
        'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
        '    .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
        'Dim Result = Query.Execute
        'Assert.AreEqual(2, Result.Count)
        'Dim Test1 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "23aee672-db40-499e-95fc-4cdb7897187e" Select i).FirstOrDefault()
        'Assert.IsNotNull(Test1)
        'Assert.AreEqual(Test1.Fields("SecondTable").Count, 2)
        'Assert.IsTrue(DirectCast(Test1.Fields("SecondTable"), List(Of Entity)).FirstOrDefault.Fields.ContainsKey("AData"))

        ''TEST 2: 1-N Two levels, Two child type
        'FirstQuery()
        'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
        '     .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID") _
        '     .Join("Thirdtable", "tt", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
        'Result = Query.Execute
        'Dim Test2_1 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "23aee672-db40-499e-95fc-4cdb7897187e" Select i).FirstOrDefault()
        'Assert.AreEqual(Test2_1.Fields("SecondTable").Count, 2)
        'Assert.AreEqual(Test2_1.Fields("Thirdtable").Count, 1)
        'Dim Test2_2 = (From i In Result Where i.Fields("ID").ToString.ToLower() = "c38be8e1-440e-4db8-a235-a35c428913bf" Select i).FirstOrDefault()
        'Assert.AreEqual(Test2_2.Fields("SecondTable").Count, 1)
        'Assert.AreEqual(Test2_2.Fields("Thirdtable").Count, 2)
        'Assert.IsTrue(DirectCast(Test2_2.Fields("Thirdtable"), List(Of Entity)).FirstOrDefault.Fields("Name").ToString().StartsWith("MyTest3_"))
        'Assert.AreEqual(DirectCast(Test2_1.Fields("Thirdtable"), List(Of Entity)).FirstOrDefault.Fields("NullableInt"), 123)

        ''TEST 3: 1-N Three levels, One child type per level
        'FirstQuery()
        'Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
        '     .Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID") _
        '     .Join("FOURTHTABLE", "frt", True).By("ParentID", RelationshipOperators.Equal, "st", "ID")
        'Result = Query.Execute



        'TEST 4
        FirstQuery()
        Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2") _
        .Join("SecondTable", "st", True, NestedEntityType.MultipleEntity).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
        Dim Result = Query.Execute




    End Sub

    <TestMethod>
    Public Sub Expand()

        '1 level after ROOT
        FirstQuery()
        Query.Expand("SecondTable", "st", "ft", "ID", "ParentID")
        Dim Result = Query.Execute
        Dim a = 0


    End Sub
End Class
