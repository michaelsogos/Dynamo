Imports Dynamo.Contracts
Imports Dynamo.Entities

Namespace Contracts
    Public Interface IRepository
        Function Query(ByVal EntityName As String, ByVal EntityAlias As String) As IQueryBuilder
        Sub AddEntity(ByRef Entity As Entity)
        Sub AddEntities(ByRef Entities As List(Of Entity))
        Sub UpdateEntity(ByRef Entity As Entity)
        Sub UpdateEntities(ByRef Entities As List(Of Entity))
        Sub DeleteEntity(ByRef Entity As Entity)
        Sub DeleteEntities(ByRef Entities As List(Of Entity))
        Sub OpenConnection()
        Sub CloseConnection()
    End Interface
End Namespace

Public MustInherit Class DynamoRepository(Of QueryBuilder As DynamoQueryBuilder)
    Implements IRepository
    Implements IDisposable

    Public ReadOnly Conventions As DynamoConventions
    Public ReadOnly ConnectionString As String

    Public Event MappingDataToEntity As EventHandler(Of DynamoMappingDataToEntityEventArgs)

    Public Sub New(ByVal ConnectionString As String)
        Me.Conventions = New DynamoConventions
        Me.ConnectionString = ConnectionString
        AddHandler MappingDataToEntity, Sub(s, e) RaiseEvent MappingDataToEntity(s, e)
    End Sub

    Public Function Query(ByVal EntityName As String, ByVal EntityAlias As String) As IQueryBuilder Implements IRepository.Query
        Return Activator.CreateInstance(GetType(QueryBuilder), {Me, EntityName, EntityAlias})
    End Function

    Public MustOverride Sub OpenConnection() Implements IRepository.OpenConnection

    Public MustOverride Sub CloseConnection() Implements IRepository.CloseConnection

    Public MustOverride Sub AddEntity(ByRef Entity As Entity) Implements IRepository.AddEntity

    Public MustOverride Sub AddEntities(ByRef Entities As List(Of Entity)) Implements IRepository.AddEntities

    Public MustOverride Sub UpdateEntity(ByRef Entity As Entity) Implements IRepository.UpdateEntity

    Public MustOverride Sub UpdateEntities(ByRef Entities As List(Of Entity)) Implements IRepository.UpdateEntities

    Public MustOverride Sub DeleteEntity(ByRef Entity As Entity) Implements IRepository.DeleteEntity

    Public MustOverride Sub DeleteEntities(ByRef Entities As List(Of Entity)) Implements IRepository.DeleteEntities

#Region "IDisposable Support"
    Private disposedValue As Boolean ' To detect redundant calls

    Protected Overridable Sub Dispose(disposing As Boolean)
        If Not Me.disposedValue Then
            If disposing Then
                'TODO Michael Sogos: Add something or remove the interface
                'CustomQueryBuilder.Dispose()
            End If

        End If
        Me.disposedValue = True
    End Sub

    Public Sub Dispose() Implements IDisposable.Dispose
        Dispose(True)
        GC.SuppressFinalize(Me)
    End Sub


#End Region

End Class

Public Class DynamoConventions

    Public Property AutodetectEntityFieldID As Boolean
    Public Property EntityFieldID As String
    Public Property EntityFieldName As String

    Sub New()
        AutodetectEntityFieldID = True
        EntityFieldID = "ID|{entityname}ID|ID{entityname}"
        EntityFieldName = "NAME|{entityname}NAME|NAME{entityname}"
    End Sub

End Class

Public Class DynamoMappingDataToEntityEventArgs
    Inherits EventArgs

    Public Entity As Entity
    Public DataSchema As Object

End Class