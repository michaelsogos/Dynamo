Imports Dynamo.Providers
Imports System.Data.SqlClient
Imports System.Dynamic
Imports System.Collections.ObjectModel
Imports Dynamo.Entities


Public Class SQLServerProvider
    Inherits DynamoProvider

    Private Const GetForeignKeysQuery As String = "SELECT OBJECT_NAME(f.constraint_object_id) AS 'ForeignKey', OBJECT_NAME(f.parent_object_id) AS 'FKTable', c1.[name] AS 'FKColumnName', OBJECT_NAME(f.referenced_object_id) AS 'PKTable', c2.[name] AS 'PKColumnName' FROM sys.foreign_key_columns f INNER JOIN sys.all_columns c1 ON f.parent_object_id = c1.[object_id] AND f.parent_column_id = c1.column_id INNER JOIN sys.all_columns c2 ON f.referenced_object_id = c2.[object_id] AND f.referenced_column_id = c2.column_id WHERE OBJECT_NAME(f.parent_object_id) = @EntityName"

    Public Sub New(ByVal ConnectionString As String)
        MyBase.New(ConnectionString)
    End Sub

    Public Overloads Overrides Function Execute(expression As Expressions.Expression) As Object
        Dim a = 0

        'Return Activator.CreateInstance(GetType(IEnumerable).MakeGenericType(expression.Type),
        '                                System.Reflection.BindingFlags.Instance Or System.Reflection.BindingFlags.NonPublic,
        '                                Nothing,
        '                                New Object() {Result},
        '                                Nothing)
    End Function

    Public Overloads Overrides Function Execute(Of TResult)(expression As Expressions.Expression) As TResult
        Dim Result As New List(Of Entity)
        Dim QueryString = Translate(expression)

        Using Connection = New SqlConnection(ConnectionString)
            Connection.Open()
            Dim DBComm As New SqlCommand(QueryString, Connection)
            Using DBRead = DBComm.ExecuteReader(CommandBehavior.KeyInfo)
                Dim DBSchema = DBRead.GetSchemaTable
                'Get entity name
                Dim EntityName As String = (From column In DBSchema.AsEnumerable Select column.Field(Of String)("BaseTableName")).FirstOrDefault
                'Check if primary key is composed by many fields
                Dim PrimaryKeyColumns = (From column In DBSchema.AsEnumerable Where column.Field(Of Boolean)("IsKey") = True).ToList
                Dim CanFillEntityID As Boolean = True
                If PrimaryKeyColumns.Count <> 1 Then CanFillEntityID = False
                'retrieve foreignkeys if needed
                If Not EntitiesRelationships.ContainsKey(EntityName) Then
                    Dim DBCommForeignKeys As New SqlCommand(GetForeignKeysQuery, Connection)
                    DBCommForeignKeys.Parameters.Add(New SqlParameter("@EntityName", EntityName))
                    Dim EntityRelationships As New List(Of EntityRelationShip)
                    Using DBReadForeignKeys = DBCommForeignKeys.ExecuteReader()
                        While DBReadForeignKeys.Read
                            Dim Relationship As New EntityRelationShip
                            Relationship.EntityName = DBReadForeignKeys("FKTable")
                            Relationship.FieldName = DBReadForeignKeys("FKColumnName")
                            Relationship.ForeignKeyName = DBReadForeignKeys("ForeignKey")
                            Relationship.PrimaryEntityName = DBReadForeignKeys("PKTable")
                            Relationship.PrimaryFieldName = DBReadForeignKeys("PKColumnName")
                            EntityRelationships.Add(Relationship)
                        End While
                    End Using
                    EntitiesRelationships.Add(EntityName, EntityRelationships)
                End If
                'retrieve query result
                While DBRead.Read
                    Dim Record As New Entity
                    Dim PrimaryKeys As New Dictionary(Of String, Object)
                    For i As Integer = 0 To DBRead.FieldCount - 1
                        Record.Fields.Add(DBRead.GetName(i), If(DBRead.IsDBNull(i), Nothing, DBRead(i)))

                        If Not DBSchema.Rows(i).IsNull("IsKey") AndAlso DBSchema.Rows(i)("IsKey") = True Then
                            If CanFillEntityID Then Record.Id = DBRead(i)
                            PrimaryKeys.Add(DBRead.GetName(i), DBRead(i))
                        End If
                        ''Retrieve NAME By Convention (PrimaryKey Column Name + "Name"), but with the possibility to change the convention (with prefix and suffix too)
                        ''Aggiungere un delegate handler per riempire una set di campi standard

                    Next
                    Record.PrimaryKeys = New ReadOnlyDictionary(Of String, Object)(PrimaryKeys)

                    Dim EventArgs As New DynamoMappingDataToEntityEventArgs
                    EventArgs.DataSchema = DBSchema
                    EventArgs.Entity = Record
                    OnMappingDataToEntity(EventArgs)
                    Result.Add(Record)
                End While
            End Using
        End Using
        Return CType(Result, IEnumerable)
    End Function

    Protected Overrides Function Translate(expression As Expressions.Expression) As String
        Return New Translators.TSQLTranslator().Translate(expression)
    End Function
End Class

