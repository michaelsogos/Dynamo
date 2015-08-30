Imports System.Dynamic
Imports Dynamo.Collections

Namespace Entities

#Region "Interfaces"
    Public Interface IEntity
        Property Id As Object
        Property Name As String
        Property Fields As Fields
        Property Schema As EntitySchema
        Property Status As EntityStatus
    End Interface

    Public Interface IEntitySchema
        Property EntityName As String
        Property FieldID As String
        Property FieldName As String
    End Interface

    Public Interface IEntityStatus
        Property Errors As List(Of String)
        Property HasErrors As Boolean
        Property FetchedOn As DateTimeOffset
        Property IssueTime As TimeSpan
    End Interface
#End Region


    Public Class Entity
        Inherits DynamicObject
        Implements IEntity

        Public Property Id As Object Implements IEntity.Id
        Public Property Name As String Implements IEntity.Name
        Public Property Fields As Fields Implements IEntity.Fields
        Public Property Schema As EntitySchema Implements IEntity.Schema
        Public Property Status As EntityStatus Implements IEntity.Status

        Sub New(ByVal EntitySchemaName As String)
            Fields = New Fields
            Schema = New EntitySchema
            Schema.EntityName = EntitySchemaName
            Status = New EntityStatus
        End Sub

        Public Overrides Function TryGetMember(ByVal binder As GetMemberBinder, ByRef result As Object) As Boolean
            Return Fields.TryGetValue(binder.Name, result)
        End Function

    End Class

    Public Class EntitySchema
        Implements IEntitySchema

        Public Property EntityName As String Implements IEntitySchema.EntityName
        Public Property FieldID As String Implements IEntitySchema.FieldID
        Public Property FieldName As String Implements IEntitySchema.FieldName
    End Class

    Public Class EntityStatus
        Implements IEntityStatus

        Public Property Errors As List(Of String) Implements IEntityStatus.Errors
        Public Property FetchedOn As DateTimeOffset Implements IEntityStatus.FetchedOn
        Public Property IssueTime As TimeSpan Implements IEntityStatus.IssueTime
        Public Property HasErrors As Boolean Implements IEntityStatus.HasErrors

        Public Sub New()
            Errors = New List(Of String)
            HasErrors = False
        End Sub
    End Class

End Namespace