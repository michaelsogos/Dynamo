Imports Dynamo.Contracts
Imports Dynamo.Entities

Namespace Contracts
    Public Interface IRepository
        Function Query(ByVal EntityName As String, ByVal EntityAlias As String) As IQueryBuilder
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
        If DynamoCache.EntitiesForeignkeys Is Nothing Then DynamoCache.EntitiesForeignkeys = New Dictionary(Of String, List(Of EntityForeignkey))
        AddHandler MappingDataToEntity, Sub(s, e) RaiseEvent MappingDataToEntity(s, e)
    End Sub

    Public Function Query(ByVal EntityName As String, ByVal EntityAlias As String) As IQueryBuilder Implements IRepository.Query
        Return Activator.CreateInstance(GetType(QueryBuilder), {Me, EntityName, EntityAlias})
    End Function

    Public MustOverride Sub OpenConnection() Implements IRepository.OpenConnection

    Public MustOverride Sub CloseConnection() Implements IRepository.CloseConnection

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
        _AutodetectEntityFieldID = False
        _EntityFieldID = ""
        _EntityFieldName = ""
    End Sub

End Class

Public Class DynamoMappingDataToEntityEventArgs
    Inherits EventArgs

    Public Entity As Entity
    Public DataSchema As Object

End Class