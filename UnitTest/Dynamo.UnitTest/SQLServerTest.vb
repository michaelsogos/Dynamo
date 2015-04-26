Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Dynamo.Providers
Imports System.Configuration
Imports Dynamo.Expressions

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

        'Test 1-N RelationShip
        FirstQuery()
        Query.FilterBy("ft", "Name", FilterOperators.Equal, "Test2").Join("SecondTable", "st", True).By("ParentID", RelationshipOperators.Equal, "ft", "ID")
        Dim Result = Query.Execute


    End Sub

End Class
