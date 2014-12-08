Imports System.Linq.Expressions
Imports System.Data.Common
Imports Dynamo.Entities

Public MustInherit Class DynamoProvider
    Implements IQueryProvider

    Protected EntityName As String
    Protected ConnectionString As String
    Public Connection As DbConnection

    Protected ReadOnly Property EntitiesRelationships As Dictionary(Of String, List(Of EntityRelationShip))
        Get
            Return DynamoCache.EntitiesRelationships
        End Get
    End Property

    Public Event MappingDataToEntity As EventHandler(Of DynamoMappingDataToEntityEventArgs)

    Public Sub New(ByVal ConnectionString As String)
        Me.ConnectionString = ConnectionString
        If DynamoCache.EntitiesRelationships Is Nothing Then DynamoCache.EntitiesRelationships = New Dictionary(Of String, List(Of EntityRelationShip))
    End Sub

    Public Function CreateQuery(expression As Expression) As IQueryable Implements IQueryProvider.CreateQuery
        Return New DynamoQueryable(Me, expression)
    End Function

    Public Function CreateQuery(Of TElement)(expression As Expression) As IQueryable(Of TElement) Implements IQueryProvider.CreateQuery
        Return DirectCast(New DynamoQueryable(Me, expression), IQueryable(Of TElement))
    End Function

    Protected Overridable Sub OnMappingDataToEntity(e As DynamoMappingDataToEntityEventArgs)
        RaiseEvent MappingDataToEntity(Me, e)
    End Sub

    Public MustOverride Function Execute(expression As Expression) As Object Implements IQueryProvider.Execute

    Public MustOverride Function Execute(Of TResult)(expression As Expression) As TResult Implements IQueryProvider.Execute

    Protected MustOverride Function Translate(ByVal expression As Expression) As String

End Class