Imports Dynamo.Entities

Public Class DynamoContext(Of provider As DynamoProvider)

    Protected ReadOnly CustomProvider As DynamoProvider

    Public Event MappingDataToEntity As EventHandler(Of DynamoMappingDataToEntityEventArgs)

    Public Sub New(ByVal ConnectionStringName As String)
        Me.CustomProvider = Activator.CreateInstance(GetType(provider), {ConnectionStringName})
        AddHandler Me.CustomProvider.MappingDataToEntity, Sub(s, e) RaiseEvent MappingDataToEntity(s, e)
    End Sub

    Public Function Entity(ByVal EntityName As String) As IQueryable(Of Entity)
        Return New DynamoQueryable(EntityName, CustomProvider)
    End Function

End Class

Public Class DynamoMappingDataToEntityEventArgs
    Inherits EventArgs

    Public Entity As Entity
    Public DataSchema As Object

End Class