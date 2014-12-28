Imports Dynamo.Entities

Public Class DynamoContext(Of provider As DynamoProvider)
    Implements IDisposable

    Protected ReadOnly CustomProvider As DynamoProvider

    Public ReadOnly Conventions As DynamoConventions

    Public Event MappingDataToEntity As EventHandler(Of DynamoMappingDataToEntityEventArgs)

    Public Sub New(ByVal ConnectionStringName As String)
        Me.Conventions = New DynamoConventions
        Me.CustomProvider = Activator.CreateInstance(GetType(provider), {ConnectionStringName, Conventions})
        AddHandler Me.CustomProvider.MappingDataToEntity, Sub(s, e) RaiseEvent MappingDataToEntity(s, e)
    End Sub

    Public Function Entity(ByVal EntityName As String) As IQueryable(Of Entity)
        Return New DynamoQueryable(Of Entity)(EntityName, CustomProvider)
    End Function


#Region "IDisposable Support"
    Private disposedValue As Boolean ' To detect redundant calls

    Protected Overridable Sub Dispose(disposing As Boolean)
        If Not Me.disposedValue Then
            If disposing Then
                CustomProvider.Dispose()
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

Public Class DynamoMappingDataToEntityEventArgs
    Inherits EventArgs

    Public Entity As Entity
    Public DataSchema As Object

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