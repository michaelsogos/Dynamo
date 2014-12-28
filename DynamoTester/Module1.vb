Imports Dynamo.Providers
Imports Dynamo
Imports System.Data.SqlClient
Imports System.Configuration
Imports System.Dynamic

Module Module1

    Sub Main()

        Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
        Using context As New DynamoContext(Of SQLServerProvider)(ConnectionString)




            AddHandler context.MappingDataToEntity, AddressOf test

            Dim a = From re In context.Entity("app_sidebarmenu")
            Dim b = From R In context.Entity("mainEntity")
                    Join J In context.Entity("joinEntity") On R.Fields("F1") Equals J.Fields("J1")
                    Where R.Fields("ItemORder") = 3 AndAlso R.Fields("LEFT") <> R.Fields("RIGHT") Or R.Fields("NOTHING") Is Nothing
                    Order By R.Fields("asc") Descending, R.Fields("desc") Ascending

            'Dim c = a.ToList
            Dim d = b.ToList

        End Using
    End Sub

    Private Sub test(sender As Object, e As DynamoMappingDataToEntityEventArgs)
        'e.Entity.Name = "fdfdf"

    End Sub

    Private Function tt() As String
        Return "f"
    End Function

End Module




