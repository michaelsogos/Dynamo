Imports System.Data.SqlClient

Public Class SQLRepository
    Inherits DynamoRepository(Of SQLQueryBuilder)

    Public Connection As SqlConnection

    Public Sub New(ByVal ConnectionString As String)
        MyBase.New(ConnectionString)
    End Sub

    Public Overrides Sub OpenConnection()
        Dim ConnectionBuilder As New SqlConnectionStringBuilder(Me.ConnectionString)
        If Not ConnectionBuilder.MultipleActiveResultSets Then
            Trace.TraceWarning("The MARS feature is disable!")
            ConnectionBuilder.MultipleActiveResultSets = True
            Trace.TraceWarning("The MARS feature forced to enable!")
        End If

        If Connection Is Nothing Then Connection = New SqlConnection(ConnectionBuilder.ConnectionString)
        If String.IsNullOrWhiteSpace(Connection.ConnectionString) Then Connection.ConnectionString = ConnectionBuilder.ConnectionString
        If Connection.State <> ConnectionState.Open Then Connection.Open()
    End Sub

    Public Overrides Sub CloseConnection()
        If Connection IsNot Nothing AndAlso Connection.State <> ConnectionState.Closed Then Connection.Close()
    End Sub
End Class