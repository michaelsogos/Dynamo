
Imports Arango.Client
Imports Dynamo.Entities

Public Class Repository
    Inherits DynamoRepository(Of QueryBuilder)

    Public ConnectionAlias As String
    Public Context As ADatabase

    Public Sub New(ByVal ConnectionString As String)
        MyBase.New(ConnectionString)
    End Sub

    Public Overrides Sub OpenConnection()
        If String.IsNullOrWhiteSpace(ConnectionAlias) Then ConnectionAlias = ConnectionBuilder()
        Context = New ADatabase(ConnectionAlias)
    End Sub

    Public Overrides Sub CloseConnection()
        'Nothing to do
    End Sub

    Private Function ConnectionBuilder() As String
        Dim ConnectionSetting As New ArangoConnectionSetting
        Dim ConnectionKeyValues = Me.ConnectionString.Split(";")
        For Each KeyValue In ConnectionKeyValues
            Dim KV = KeyValue.Split("=")
            Select Case KV(0).ToLower
                Case "database"
                    ConnectionSetting.Database = KV(1)
                Case "password"
                    ConnectionSetting.Password = KV(1)
                Case "user"
                    ConnectionSetting.Username = KV(1)
                Case "server"
                    ConnectionSetting.Server = KV(1)
                Case "port"
                    If Not Single.TryParse(KV(1), ConnectionSetting.Port) Then Throw New InvalidCastException(String.Format("Cannot cast the connection string [Port] value from {0} to a number!", KV(1)))
                Case "issecure"
                    If Not Boolean.TryParse(KV(1), ConnectionSetting.IsSecure) Then Throw New InvalidCastException(String.Format("Cannot cast the connection string [IsSecure] value from {0} to a boolean!", KV(1)))
            End Select
        Next

        If String.IsNullOrWhiteSpace(ConnectionSetting.Server) Then Throw New Exception("The connection string parameter [Server] cannot be empty!")
        If ConnectionSetting.Port <= 0 Then ConnectionSetting.Port = 8529

        If String.IsNullOrWhiteSpace(ConnectionSetting.Database) Then
            If (String.IsNullOrWhiteSpace(ConnectionSetting.Username) OrElse String.IsNullOrWhiteSpace(ConnectionSetting.Password)) Then
                ASettings.AddConnection(ConnectionSetting.ConnectionAlias, ConnectionSetting.Server, ConnectionSetting.Port, ConnectionSetting.IsSecure)
            Else
                ASettings.AddConnection(ConnectionSetting.ConnectionAlias, ConnectionSetting.Server, ConnectionSetting.Port, ConnectionSetting.IsSecure, ConnectionSetting.Username, ConnectionSetting.Password)
            End If
        Else
            If (String.IsNullOrWhiteSpace(ConnectionSetting.Username) OrElse String.IsNullOrWhiteSpace(ConnectionSetting.Password)) Then
                ASettings.AddConnection(ConnectionSetting.ConnectionAlias, ConnectionSetting.Server, ConnectionSetting.Port, ConnectionSetting.IsSecure, ConnectionSetting.Database)
            Else
                ASettings.AddConnection(ConnectionSetting.ConnectionAlias, ConnectionSetting.Server, ConnectionSetting.Port, ConnectionSetting.IsSecure, ConnectionSetting.Database, ConnectionSetting.Username, ConnectionSetting.Password)
            End If
        End If

        Return ConnectionSetting.ConnectionAlias
    End Function

    Friend Function ConvertResultToEntity(ByRef EntitySchemaName As String, ByRef Result As IDictionary(Of String, Object)) As Entity
        Dim Record = New Entity(EntitySchemaName)
        Record.Schema.PrimaryFieldID = "_key"
        Record.Id = Result("_key")
        Record.Fields.AddRange(Result)
        Return Record
    End Function

    Friend Sub RetrieveSchemaFieldName(ByRef ArangoEntity As Dictionary(Of String, Object), ByRef Record As Entity)
        Dim LookupNames = Me.Conventions.EntityFieldName.Replace("{entityname}", Record.Schema.EntityName).ToLower().Split("|")
        Dim FieldName = (From FieldKey In ArangoEntity.Keys Where LookupNames.Contains(FieldKey.ToLower) Select FieldKey).FirstOrDefault
        If Not String.IsNullOrWhiteSpace(FieldName) Then
            Record.Schema.PrimaryFieldName = FieldName
            Record.Name = ArangoEntity(FieldName)
        Else
            Record.Status.Errors.Add("Cannot retrieve the ENTITY NAME by using convention.")
        End If
    End Sub

#Region "Create, Update, Delete operations"
    Public Overrides Sub AddEntity(ByRef Entity As Entity)
        If String.IsNullOrWhiteSpace(Entity.Schema.EntityName) Then Throw New NullReferenceException("The entity schema name is empty.")

        Me.OpenConnection()
        Dim Result = Me.Context.Document.WaitForSync(True).Create(Entity.Schema.EntityName, Entity.Fields)
        If Result.Error IsNot Nothing Then Throw New Exception(Result.Error.Message)
        If Result.Success Then
            'TODO Michael Sogos 2015-08-26: Add something to retrieve the entire entity from db, the default is retrieve only the ID/_key because the assumption is that on create stage the entity cannot be different from the original 
            'Entity = ConvertResultToEntity(Entity.Schema.EntityName, Result.Value)
            Entity.Fields("_key") = Result.Value("_key")
            Entity.Fields("_rev") = Result.Value("_rev")
            Entity.Fields("_id") = Result.Value("_id")
            Entity.Schema.PrimaryFieldID = "_key"
            Entity.Id = Result.Value("_key")
            RetrieveSchemaFieldName(Entity.Fields, Entity)
        Else
            Throw New Exception("The database return an unmanaged error.")
        End If
    End Sub

    Public Overrides Sub AddEntities(ByRef Entities As List(Of Entity))
        For Each Entity In Entities
            AddEntity(Entity)
        Next
    End Sub

    Public Overrides Sub UpdateEntity(ByRef Entity As Entity)
        If String.IsNullOrWhiteSpace(Entity.Schema.EntityName) Then Throw New NullReferenceException("The entity schema name is empty.")
        If String.IsNullOrWhiteSpace(Entity.Id) Then Throw New NullReferenceException("The entity id is empty.")
        Dim ID = String.Format("{0}/{1}", Entity.Schema.EntityName, Entity.Id)
        Me.OpenConnection()
        Dim Result = Me.Context.Document.WaitForSync(True).KeepNull(False).MergeObjects(True).Update(ID, Entity.Fields)
        If Result.Error IsNot Nothing Then Throw New Exception(Result.Error.Message)
    End Sub

    Public Overrides Sub UpdateEntities(ByRef Entities As List(Of Entity))
        For Each Entity In Entities
            UpdateEntity(Entity)
        Next
    End Sub

    Public Overrides Sub DeleteEntity(ByRef Entity As Entity)
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub DeleteEntities(ByRef Entities As List(Of Entity))
        Throw New NotImplementedException()
    End Sub
#End Region
End Class

Class ArangoConnectionSetting
    Public Property ConnectionAlias As String
    Public Property Server As String
    Public Property Port As Single
    Public Property IsSecure As Boolean
    Public Property Database As String
    Public Property Username As String
    Public Property Password As String

    Public Sub New()
        ConnectionAlias = Guid.NewGuid.ToString()
    End Sub
End Class