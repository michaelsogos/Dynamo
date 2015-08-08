Imports Dynamo
Imports Dynamo.Providers
Imports System.Configuration
Imports Newtonsoft.Json

Module Module1

    'Sub Main()

    '    Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
    '    Using context As New DynamoContext(Of SQLServerProvider)(ConnectionString)


    '        'AddHandler context.MappingDataToEntity, AddressOf test

    '        Dim a = From re In context.Db("DATA_BASE").Query("app_SidebarApp") Where re.Name = ""

    '        Dim b = From R In context.Db("DATA_BASE").Query("mainEntity")
    '                Join J In context.Db("DATA_BASE").Query("joinEntity") On R.Fields("F1") Equals J.Fields("J1")
    '                Where R.Fields("ItemORder") = 3 AndAlso R.Fields("LEFT") <> R.Fields("RIGHT") Or R.Fields("NOTHING") Is Nothing AndAlso J.Name = ""
    '                Order By R.Fields("asc") Descending, R.Fields("desc") Ascending

    '        'Dim c = a.ToList
    '        Dim d = b.ToList

    '    End Using
    'End Sub

    'Sub Main()

    '    Dim Ctx As New CContext
    '    'Dim a = Ctx.Query.a
    '    'Dim b = Ctx.Query.getB("b")

    '    Ctx.Query("TEST")

    'End Sub

    Sub Main()
        'Dim ConnectionString = ConfigurationManager.ConnectionStrings("DBTest").ConnectionString
        'Dim Repo As New SQLRepository(ConnectionString)
        'Dim Result = Repo.Query("app_sidebarmenu", "sm").FilterBy("sm", "itemorder", FilterOperators.Include + FilterOperators.Not, New String() {1, 3}).Execute()

        'Dim b = Result.FirstOrDefault
        'Dim c = JsonConvert.SerializeObject(Result)

        '' '' '' ''Dim E1 As New Entities.Entity
        '' '' '' ''Dim E2 As Object = New Entities.SEntity
        '' '' '' ''Dim R As New Random(10)

        ' '' '' '' ''WRITE
        '' '' '' ''Dim stopWatch As New Stopwatch()
        '' '' '' ''stopWatch.Start()
        '' '' '' ''For x As Long = 1 To 1000000
        '' '' '' ''    E1.Fields("TEST1") = x
        '' '' '' ''    E1.Fields("TEST2") = x + R.Next()
        '' '' '' ''    'E1.Fields.Add("TEST" + x.ToString, x)
        '' '' '' ''Next
        '' '' '' ''stopWatch.Stop()
        '' '' '' ''Dim ts = stopWatch.Elapsed
        '' '' '' ''Dim elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10)
        '' '' '' ''Console.WriteLine("WRITE 1.000.000 E1: " + elapsedTime)

        '' '' '' ''stopWatch = New Stopwatch()
        '' '' '' ''stopWatch.Start()
        '' '' '' ''For x As Long = 1 To 1000000
        '' '' '' ''    E2.TEST1 = x
        '' '' '' ''    E2.TEST2 = x + R.Next
        '' '' '' ''Next
        '' '' '' ''stopWatch.Stop()
        '' '' '' ''ts = stopWatch.Elapsed
        '' '' '' ''elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10)
        '' '' '' ''Console.WriteLine("WRITE 1.000.000 E2: " + elapsedTime)

        '' '' '' ''Console.ReadKey()

        ' '' '' '' ''READ
        '' '' '' ''stopWatch = New Stopwatch()
        '' '' '' ''stopWatch.Start()
        '' '' '' ''For x = 1 To 1000000
        '' '' '' ''    Dim a = E1.Fields("TEST1")
        '' '' '' ''    Dim b = E1.Fields("TEST2")
        '' '' '' ''Next
        '' '' '' ''stopWatch.Stop()
        '' '' '' ''ts = stopWatch.Elapsed
        '' '' '' ''elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10)
        '' '' '' ''Console.WriteLine("READ 1.000.000 E1: " + elapsedTime)


        '' '' '' ''stopWatch = New Stopwatch()
        '' '' '' ''stopWatch.Start()
        '' '' '' ''For x = 1 To 1000000
        '' '' '' ''    Dim a = E2.TEST1
        '' '' '' ''    Dim b = E2.TEST2
        '' '' '' ''Next
        '' '' '' ''stopWatch.Stop()
        '' '' '' ''ts = stopWatch.Elapsed
        '' '' '' ''elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10)
        '' '' '' ''Console.WriteLine("READ 1.000.000 E2: " + elapsedTime)

        '' '' '' ''Console.ReadKey()

    End Sub



End Module




'Public Class CContext

'    Private _SQL As String = ""

'    Public Function Query() As Object 'For Dynamic Query
'        Return New CEntity
'    End Function

'    Public Function Query(ByVal EntityName As String) As CContext 'For Fluent Interface
'        _SQL += "SELECT * FROM " + EntityName
'        Return Me
'    End Function

'End Class

'Public Class CEntity
'    Inherits DynamicObject

'    Public Overrides Function TryGetMember(binder As GetMemberBinder, ByRef result As Object) As Boolean
'        Return MyBase.TryGetMember(binder, result)
'    End Function

'    Public Overrides Function TryInvoke(binder As InvokeBinder, args() As Object, ByRef result As Object) As Boolean
'        Return MyBase.TryInvoke(binder, args, result)
'    End Function

'End Class




