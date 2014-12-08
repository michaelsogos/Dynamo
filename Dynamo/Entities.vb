Imports System.Dynamic
Imports System.Collections.ObjectModel

Namespace Entities
    Public Class Entity
        Public Property Id As Object
        Public Property Name As String
        Public Property Entity As String
        Public Fields As Dictionary(Of String, Object)
        Public PrimaryKeys As ReadOnlyDictionary(Of String, Object)

        Sub New()
            Fields = New Dictionary(Of String, Object)
        End Sub
    End Class

    Public Class EntityRelationShip
        Public Property ForeignKeyName As String
        Public Property EntityName As String
        Public Property FieldName As String
        Public Property PrimaryEntityName As String
        Public Property PrimaryFieldName As String
    End Class

End Namespace