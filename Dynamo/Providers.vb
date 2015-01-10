Imports System.Linq.Expressions
Imports System.Data.Common
Imports Dynamo.Entities

Public MustInherit Class DynamoProvider
    Implements IQueryProvider    
    Implements IDisposable

    Protected EntityName As String
    Protected ConnectionString As String
    Public ReadOnly Conventions As DynamoConventions
    Public Connection As DbConnection

    Protected ReadOnly Property EntitiesRelationships As Dictionary(Of String, List(Of EntityRelationShip))
        Get
            Return DynamoCache.EntitiesRelationships
        End Get
    End Property

    Public Event MappingDataToEntity As EventHandler(Of DynamoMappingDataToEntityEventArgs)

    Public Sub New(ByVal ConnectionString As String, ByRef Conventions As DynamoConventions)
        Me.ConnectionString = ConnectionString
        Me.Conventions = Conventions
        If DynamoCache.EntitiesRelationships Is Nothing Then DynamoCache.EntitiesRelationships = New Dictionary(Of String, List(Of EntityRelationShip))
    End Sub

    Public Function CreateQuery(expression As Expression) As IQueryable Implements IQueryProvider.CreateQuery
        Return New DynamoQueryable(Of Entity)(Me, expression)
        'Throw New NotImplementedException
    End Function

    Public Function CreateQuery(Of TElement)(expression As Expression) As IQueryable(Of TElement) Implements IQueryProvider.CreateQuery
        Return New DynamoQueryable(Of TElement)(Me, expression)
    End Function

    Protected Overridable Sub OnMappingDataToEntity(e As DynamoMappingDataToEntityEventArgs)
        RaiseEvent MappingDataToEntity(Me, e)
    End Sub

    Public MustOverride Function Execute(expression As Expression) As Object Implements IQueryProvider.Execute

    Public MustOverride Function Execute(Of TResult)(expression As Expression) As TResult Implements IQueryProvider.Execute

    Protected MustOverride Function Translate(ByVal expression As Expression) As String


#Region "IDisposable Support"
    Protected MustOverride Sub Dispose(disposing As Boolean)

    Public Sub Dispose() Implements IDisposable.Dispose
        Dispose(True)
        GC.SuppressFinalize(Me)
    End Sub
#End Region

End Class