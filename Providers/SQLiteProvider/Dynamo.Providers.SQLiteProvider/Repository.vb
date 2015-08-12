
Imports System.Data.SQLite

Public Class Repository
    Inherits DynamoRepository(Of QueryBuilder)

    Public Connection As SQLiteConnection

    Public Sub New(ByVal ConnectionString As String)
        MyBase.New(ConnectionString)
    End Sub

    Public Overrides Sub OpenConnection()
        Dim ConnectionBuilder As New SQLiteConnectionStringBuilder(Me.ConnectionString)
        If Connection Is Nothing Then Connection = New SQLiteConnection(ConnectionBuilder.ConnectionString)
        If String.IsNullOrWhiteSpace(Connection.ConnectionString) Then Connection.ConnectionString = ConnectionBuilder.ConnectionString
        If Connection.State <> ConnectionState.Open Then Connection.Open()
    End Sub

    Public Overrides Sub CloseConnection()
        If Connection IsNot Nothing AndAlso Connection.State <> ConnectionState.Closed Then Connection.Close()
    End Sub
End Class