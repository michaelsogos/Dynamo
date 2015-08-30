
Imports Dynamo.Entities
Imports NuoDb.Data.Client

Public Class Repository
    Inherits DynamoRepository(Of QueryBuilder)

    Public Connection As NuoDbConnection

    Public Sub New(ByVal ConnectionString As String)
        MyBase.New(ConnectionString)
    End Sub

    Public Overrides Sub OpenConnection()
        Dim ConnectionBuilder As New NuoDbConnectionStringBuilder(Me.ConnectionString)
        If Connection Is Nothing Then Connection = New NuoDbConnection(ConnectionBuilder.ConnectionString)
        If String.IsNullOrWhiteSpace(Connection.ConnectionString) Then Connection.ConnectionString = ConnectionBuilder.ConnectionString
        If Connection.State <> ConnectionState.Open Then Connection.Open()
    End Sub

    Public Overrides Sub CloseConnection()
        If Connection IsNot Nothing AndAlso Connection.State <> ConnectionState.Closed Then Connection.Close()
    End Sub
#Region "Create, Update, Delete operations"
    Public Overrides Sub AddEntity(ByRef Entity As Entity)
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub AddEntities(ByRef Entities As List(Of Entity))
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub UpdateEntity(ByRef Entity As Entity)
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub UpdateEntities(ByRef Entities As List(Of Entity))
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub DeleteEntity(ByRef Entity As Entity)
        Throw New NotImplementedException()
    End Sub

    Public Overrides Sub DeleteEntities(ByRef Entities As List(Of Entity))
        Throw New NotImplementedException()
    End Sub
#End Region
End Class