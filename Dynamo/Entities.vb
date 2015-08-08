Imports System.Dynamic
Imports System.Collections.ObjectModel

Namespace Entities
    Public Interface IEntity
        Property Id As Object
        Property Name As String
        Property Fields As Dictionary(Of String, Object)
        Property Schema As EntitySchema
    End Interface

    Public Class Entity
        Implements IEntity

        Public Property Id As Object Implements IEntity.Id
        Public Property Name As String Implements IEntity.Name
        Public Property Fields As Dictionary(Of String, Object) Implements IEntity.Fields
        Public Property Schema As EntitySchema Implements IEntity.Schema

        Sub New()
            Fields = New Dictionary(Of String, Object)(StringComparer.InvariantCultureIgnoreCase)
            Schema = New EntitySchema
        End Sub
    End Class

    Public Class EntitySchema
        Public Property EntityName As String
        Public Property FieldID As String
        Public Property FieldName As String
        Public Property Errors As List(Of String)

        Public Sub New()
            Errors = New List(Of String)
        End Sub

    End Class

End Namespace