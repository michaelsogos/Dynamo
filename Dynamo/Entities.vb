Imports System.ComponentModel
Imports System.Dynamic
Imports Dynamo.Collections

Namespace Entities

#Region "Interfaces"
    Public Interface IEntity
        Property Id As Object
        Property Name As String
        Default Property Field(ByVal Key As String)
        ReadOnly Property Fields As Fields
        ReadOnly Property Schema As EntitySchema
        ReadOnly Property Status As EntityStatus
    End Interface

    Public Interface IEntitySchema
        Property EntityName As String
        Property PrimaryFieldID As String
        Property PrimaryFieldName As String
    End Interface

    Public Interface IEntityStatus
        Property Errors As List(Of String)
        Property HasErrors As Boolean
        Property FetchedOn As DateTimeOffset
        Property IssueTime As TimeSpan
        Property StatusCode As EntityStatusCode
    End Interface
#End Region

    Public Enum EntityStatusCode
        NotChanged = 0
        Changed = 1
    End Enum

    Public Class Entity
        Inherits DynamicObject
        Implements IEntity
        Implements INotifyPropertyChanged
        Implements INotifyPropertyChanging

#Region "Fields and Properties"
        Private _Id As Object
        Public Property Id As Object Implements IEntity.Id
            Get
                Return _Id
            End Get
            Set(value As Object)
                _Id = value
                If Not String.IsNullOrWhiteSpace(Me.Schema.PrimaryFieldID) Then Fields(Me.Schema.PrimaryFieldID) = value
            End Set
        End Property
        Public Property Name As String Implements IEntity.Name
            Get
                If String.IsNullOrWhiteSpace(Me.Schema.PrimaryFieldName) Then Throw New Exception("Cannot map the property 'Name' to a field.")
                Return Fields(Me.Schema.PrimaryFieldName)
            End Get
            Set(value As String)
                If String.IsNullOrWhiteSpace(Me.Schema.PrimaryFieldName) Then Throw New Exception("Cannot map the property 'Name' to a field.")
                Fields(Me.Schema.PrimaryFieldName) = value
            End Set
        End Property
        Default Public Property Field(Key As String) As Object Implements IEntity.Field
            Get
                Return Me.Fields(Key)
            End Get
            Set(value As Object)
                Fields(Key) = value
            End Set
        End Property
        Public ReadOnly Property Fields As Fields Implements IEntity.Fields
        Public ReadOnly Property Schema As EntitySchema Implements IEntity.Schema
        Public ReadOnly Property Status As EntityStatus Implements IEntity.Status
#End Region

        Sub New(ByVal EntitySchemaName As String)
            Fields = New Fields
            Schema = New EntitySchema
            Schema.EntityName = EntitySchemaName
            Status = New EntityStatus

            AddHandler Me.PropertyChanged, AddressOf Me.HandlePropertyChanges
            AddHandler Me.PropertyChanging, AddressOf Me.HandlePropertyChanging
        End Sub

        Public Event PropertyChanged As PropertyChangedEventHandler Implements INotifyPropertyChanged.PropertyChanged
        Public Event PropertyChanging As PropertyChangingEventHandler Implements INotifyPropertyChanging.PropertyChanging

        Public Sub HandlePropertyChanges(ByVal sender As Object, ByVal e As PropertyChangedEventArgs)
            Dim a = 0
        End Sub

        Public Sub HandlePropertyChanging(ByVal sender As Object, ByVal e As PropertyChangingEventArgs)
            If Fields.ContainsKey(e.PropertyName) Then
                Me.Status.StatusCode = EntityStatusCode.Changed
            End If
        End Sub

#Region "Dynamic Object Functions"
        Public Overrides Function TryGetMember(ByVal binder As GetMemberBinder, ByRef result As Object) As Boolean
            Return Fields.TryGetValue(binder.Name, result)
        End Function

        Public Overrides Function TrySetMember(binder As SetMemberBinder, value As Object) As Boolean
            RaiseEvent PropertyChanging(Me, New PropertyChangingEventArgs(binder.Name))
            Fields(binder.Name) = value
            RaiseEvent PropertyChanged(Me, New PropertyChangedEventArgs(binder.Name))
            Return True
        End Function
#End Region

    End Class

    Public Class EntitySchema
        Implements IEntitySchema

        Public Property EntityName As String Implements IEntitySchema.EntityName
        Public Property PrimaryFieldID As String Implements IEntitySchema.PrimaryFieldID
        Public Property PrimaryFieldName As String Implements IEntitySchema.PrimaryFieldName
    End Class

    Public Class EntityStatus
        Implements IEntityStatus

        Public Property Errors As List(Of String) Implements IEntityStatus.Errors
        Public Property FetchedOn As DateTimeOffset Implements IEntityStatus.FetchedOn
        Public Property IssueTime As TimeSpan Implements IEntityStatus.IssueTime
        Public Property HasErrors As Boolean Implements IEntityStatus.HasErrors
        Public Property StatusCode As EntityStatusCode Implements IEntityStatus.StatusCode

        Public Sub New()
            Errors = New List(Of String)
            HasErrors = False
        End Sub
    End Class

End Namespace