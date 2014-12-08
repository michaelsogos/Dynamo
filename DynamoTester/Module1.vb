Imports Dynamo.Providers
Imports Dynamo
Imports System.Data.SqlClient
Imports System.Configuration
Imports System.Dynamic

Module Module1

    Sub Main()

        Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
        Dim context As New DynamoContext(Of SQLServerProvider)(ConnectionString)

        AddHandler context.MappingDataToEntity, AddressOf test

        Dim a = From re In context.Entity("app_sidebarmenu")

        Dim c = a.ToList


    End Sub

    Private Sub test(sender As Object, e As DynamoMappingDataToEntityEventArgs)
        'e.Entity.Name = "fdfdf"

    End Sub

End Module




