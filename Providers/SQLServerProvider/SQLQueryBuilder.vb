Imports System.Text
Imports Dynamo.Contracts
Imports System.Data.SqlClient
Imports Dynamo.Entities
Imports System.Collections.ObjectModel
Imports Dynamo.Expressions

Public Class SQLQueryBuilder
    Inherits DynamoQueryBuilder

    Private Const GetForeignKeysQuery As String = "SELECT OBJECT_NAME(f.constraint_object_id) AS 'ForeignKey', OBJECT_NAME(f.parent_object_id) AS 'FKTable', c1.[name] AS 'FKColumnName', OBJECT_NAME(f.referenced_object_id) AS 'PKTable', c2.[name] AS 'PKColumnName' FROM sys.foreign_key_columns f INNER JOIN sys.all_columns c1 ON f.parent_object_id = c1.[object_id] AND f.parent_column_id = c1.column_id INNER JOIN sys.all_columns c2 ON f.referenced_object_id = c2.[object_id] AND f.referenced_column_id = c2.column_id WHERE OBJECT_NAME(f.parent_object_id) = @EntityName"

    Private TempBuilder As StringBuilder
    Private ParamNameBuilder As StringBuilder
    Private QueryBuilder As StringBuilder
    Private FilterBuilder As StringBuilder
    Private SortBuilder As StringBuilder
    Private JoinBuilder As StringBuilder
    Private Parameters As List(Of SqlParameter)
    Private EntitiesAlias As Dictionary(Of String, String)
    Private EntitiesRelationships As List(Of EntitiesRelationship)

    Public Sub New(ByRef Repository As SQLRepository, ByVal EntityName As String, ByVal EntityAlias As String)
        MyBase.New(Repository, EntityName, EntityAlias)

        Parameters = New List(Of SqlParameter)
        TempBuilder = New StringBuilder
        ParamNameBuilder = New StringBuilder
        QueryBuilder = New StringBuilder
        FilterBuilder = New StringBuilder
        SortBuilder = New StringBuilder
        JoinBuilder = New StringBuilder
        EntitiesAlias = New Dictionary(Of String, String)()
        EntitiesRelationships = New List(Of EntitiesRelationship)()

        EntitiesAlias.Add(EntityAlias, EntityName)
        QueryBuilder.AppendFormat("SELECT * FROM [{0}] {1}", EntityName, EntityAlias)
    End Sub

    Private Function FilterOperatorToString([Operator] As FilterOperators) As String
        Select Case [Operator]
            Case FilterOperators.Equal
                Return "="
            Case FilterOperators.Not + FilterOperators.Equal
                Return "<>"
            Case FilterOperators.GreaterThan _
                , FilterOperators.Not + FilterOperators.LessThan + FilterOperators.Equal
                Return ">"
            Case FilterOperators.Not + FilterOperators.GreaterThan _
                , FilterOperators.LessThan + FilterOperators.Equal
                Return "<="
            Case FilterOperators.GreaterThan + FilterOperators.Equal _
                , FilterOperators.Not + FilterOperators.LessThan
                Return ">="
            Case FilterOperators.Not + FilterOperators.GreaterThan + FilterOperators.Equal _
                , FilterOperators.LessThan
                Return "<"
            Case FilterOperators.Pattern
                Return "LIKE"
            Case FilterOperators.Not + FilterOperators.Pattern
                Return "NOT LIKE"
            Case FilterOperators.Include
                Return "IN"
            Case FilterOperators.Not + FilterOperators.Include
                Return "NOT IN"
            Case Else
                Throw New NotImplementedException("FilterOperator " + [Operator].ToString + " not implemented!")
        End Select
    End Function

    Private Function SortDirectionToString(Direction As SortDirections) As String
        Select Case Direction
            Case SortDirections.Ascending
                Return "ASC"
            Case SortDirections.Descending
                Return "DESC"
            Case Else
                Throw New NotImplementedException("SortDirection " + Direction.ToString + " not implemented!")
        End Select
    End Function

    Private Sub ConvertValueToParameter(ByRef Value As Object, ByRef [Operator] As FilterOperators)
        Select Case [Operator]
            Case FilterOperators.Include, FilterOperators.Not + FilterOperators.Include
                If Not TypeOf Value Is String() AndAlso Not TypeOf Value Is IEnumerable(Of String) Then
                    Throw New ArgumentException("The INCLUDE operator need an Array or an IEnumerable Of Strings as parameter Value!")
                End If
        End Select

        Select Case True
            Case TypeOf Value Is String
                Parameters.Add(New SqlParameter(GetNextParamName, Value))
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Integer, TypeOf Value Is Int32, TypeOf Value Is Int16, TypeOf Value Is Int64, TypeOf Value Is UInteger, TypeOf Value Is UInt16, TypeOf Value Is UInt32, TypeOf Value Is UInt64 _
                 , TypeOf Value Is Short, TypeOf Value Is UShort, TypeOf Value Is Long, TypeOf Value Is ULong, TypeOf Value Is SByte, TypeOf Value Is Byte
                Parameters.Add(New SqlParameter(GetNextParamName, Value))
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Decimal, TypeOf Value Is Double, TypeOf Value Is Single
                Parameters.Add(New SqlParameter(GetNextParamName, Value))
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Boolean
                Parameters.Add(New SqlParameter(GetNextParamName, Value))
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is DateTime, TypeOf Value Is DateTimeOffset, TypeOf Value Is Date
                Parameters.Add(New SqlParameter(GetNextParamName, Value))
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is IEnumerable(Of String)
                'For performance and limits imposed by tsql engine the list cannot be greater than 2000 items
                Dim CastedValue = DirectCast(Value, IEnumerable(Of String))
                If CastedValue.Count > 2000 Then Throw New ArgumentException("The Value parameter cannot be a list greater than 2000 items!")

                FilterBuilder.Append("(")
                Dim Counter = 0
                For Each Item In CastedValue
                    Counter += 1
                    Parameters.Add(New SqlParameter(GetNextParamName, Item))
                    If CastedValue.Count = Counter Then
                        FilterBuilder.Append(ParamNameBuilder.ToString)
                    Else
                        FilterBuilder.AppendFormat("{0},", ParamNameBuilder.ToString)
                    End If
                Next
                FilterBuilder.Append(")")
                '' Seems that String() is an IEnumerable(Of String) too.
                'Case TypeOf Value Is String()
                '    'For performance and limits imposed by tsql engine the list cannot be greater than 2000 items
                '    Dim CastedValue = DirectCast(Value, String())
                '    If CastedValue.Length > 2000 Then Throw New ArgumentException("The Value parameter cannot be a list greater than 2000 items!")

                '    WhereBuilder.Append("(")
                '    Dim Counter = 0
                '    For Each Item In CastedValue
                '        Counter += 1
                '        Parameters.Add(New SqlParameter(GetNextParamName, Item))
                '        If CastedValue.Length = Counter Then
                '            WhereBuilder.Append(ParamNameBuilder.ToString)
                '        Else
                '            WhereBuilder.AppendFormat("{0},", ParamNameBuilder.ToString)
                '        End If
                '    Next
                '    WhereBuilder.Append(")")
            Case Else
                Throw New NotImplementedException("The filter value of type " + Value.GetType.Name + " is not implemented yet!")
        End Select

    End Sub

    Private Function GetNextParamName() As String
        ParamNameBuilder.Clear()
        ParamNameBuilder.AppendFormat("@p{0}", Parameters.Count)
        Return ParamNameBuilder.ToString
    End Function

    Private Sub ParseFilterExpressionList(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), ByRef Combiner As FilterCombiners)
        Dim Counter As Integer = 0
        For Each Expression In FilterExpressions
            Counter += 1
            FilterBuilder.AppendFormat("[{0}].[{1}] {2} ", Expression.EntityAlias, Expression.FieldName, FilterOperatorToString(Expression.Operator))
            ConvertValueToParameter(Expression.Value, Expression.Operator)
            If Counter < FilterExpressions.Count Then
                Select Case Combiner
                    Case FilterCombiners.And
                        FilterBuilder.Append(" AND ")
                    Case FilterCombiners.Or
                        FilterBuilder.Append(" OR ")
                    Case Else
                        Throw New NotImplementedException("FilterCombiner " + Combiner.ToString + " not implemented!")
                End Select
            End If
        Next
    End Sub

    Public Overrides Function FilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE ")
            FilterBuilder.AppendFormat("[{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        Else
            FilterBuilder.AppendFormat(" AND [{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])
        Return Me
    End Function

    Public Overrides Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE ")
            FilterBuilder.AppendFormat("[{0}].[{1}] {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        Else
            FilterBuilder.AppendFormat(" AND [{0}].[{1}] {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value, FilterExpression.Operator)
        Return Me
    End Function

    Public Overrides Function Execute() As List(Of Entity)
        Dim Result As New List(Of Entity)
        Dim EntityFieldID As String = Nothing
        Dim EntityFieldName As String = Nothing

        TempBuilder.Clear()
        TempBuilder.Append(QueryBuilder).Append(JoinBuilder.ToString).Append(FilterBuilder.ToString).Append(SortBuilder.ToString)

        With DirectCast(Repository, SQLRepository)
            .OpenConnection()
            Using .Connection
                Using Command As New SqlCommand(TempBuilder.ToString, .Connection)
                    Command.Parameters.AddRange(Parameters.ToArray)

                    Using Reader = Command.ExecuteReader(CommandBehavior.KeyInfo)
                        Dim DBSchema = Reader.GetSchemaTable
                        'Get entity name
                        Dim EntityName As String = (From column In DBSchema.AsEnumerable Select column.Field(Of String)("BaseTableName")).FirstOrDefault
                        'Check if primary key is composed by many fields
                        Dim PrimaryKeyColumns = (From column In DBSchema.AsEnumerable Where column.Field(Of Boolean)("IsKey") = True).ToList
                        'set knowed fields names
                        'TODO Michael Sogos: Capire come interagisce LINQ con una risultato ANONIMO, tipo con le join
                        If .Conventions.AutodetectEntityFieldID AndAlso PrimaryKeyColumns.Count = 1 Then
                            EntityFieldID = PrimaryKeyColumns.FirstOrDefault.Field(Of String)("ColumnName")
                        ElseIf String.IsNullOrWhiteSpace(.Conventions.EntityFieldID) Then
                            EntityFieldID = RegularExpressions.Regex.Replace(.Conventions.EntityFieldID, "{entityobjectname}", EntityName, RegularExpressions.RegexOptions.IgnoreCase)
                            EntityFieldID = RegularExpressions.Regex.Replace(.Conventions.EntityFieldID, "{entityfieldname}", EntityName, RegularExpressions.RegexOptions.IgnoreCase)
                        End If

                        If String.IsNullOrWhiteSpace(.Conventions.EntityFieldName) Then
                            EntityFieldName = RegularExpressions.Regex.Replace(.Conventions.EntityFieldName, "{entityobjectname}", EntityName, RegularExpressions.RegexOptions.IgnoreCase)
                            EntityFieldName = RegularExpressions.Regex.Replace(.Conventions.EntityFieldName, "{entityfieldid}", EntityName, RegularExpressions.RegexOptions.IgnoreCase)
                        End If

                        'retrieve foreignkeys if needed
                        If Not EntitiesForeignkeys.ContainsKey(EntityName) Then
                            Using DBCommForeignKeys As New SqlCommand(GetForeignKeysQuery, .Connection)
                                DBCommForeignKeys.Parameters.Add(New SqlParameter("@EntityName", EntityName))
                                Dim EntityForeignkeys As New List(Of EntityForeignkey)
                                Using DBReadForeignKeys = DBCommForeignKeys.ExecuteReader()
                                    While DBReadForeignKeys.Read
                                        Dim Foreignkey As New EntityForeignkey
                                        Foreignkey.EntityName = DBReadForeignKeys("FKTable")
                                        Foreignkey.FieldName = DBReadForeignKeys("FKColumnName")
                                        Foreignkey.ForeignKeyName = DBReadForeignKeys("ForeignKey")
                                        Foreignkey.PrimaryEntityName = DBReadForeignKeys("PKTable")
                                        Foreignkey.PrimaryFieldName = DBReadForeignKeys("PKColumnName")
                                        EntityForeignkeys.Add(Foreignkey)
                                    End While
                                End Using
                                EntitiesForeignkeys.Add(EntityName, EntityForeignkeys)
                            End Using
                        End If

                        'retrieve query result
                        While Reader.Read

                            Dim SkipFieldsByEntityNames As New List(Of String)
                            Dim HaveToAddNewEntity As Boolean = True
                            Dim PrimaryKeys As New Dictionary(Of String, Object)

                            Dim Record = CheckIfEntityAlreadyExist(MainEntity, Result, Reader)
                            If Record Is Nothing Then
                                Record = New Entity
                                Record.Schema.EntityObjectName = EntityName
                            Else
                                SkipFieldsByEntityNames.Add(MainEntity)
                                HaveToAddNewEntity = False
                                PrimaryKeys = New Dictionary(Of String, Object)(Record.Schema.PrimaryKeys)
                            End If

                            For i As Integer = 0 To Reader.FieldCount - 1
                                Dim ColumnEntityName = DBSchema.Rows(i)("BaseTableName")
                                If Not String.IsNullOrWhiteSpace(ColumnEntityName) AndAlso SkipFieldsByEntityNames.Contains(ColumnEntityName) Then Continue For

                                'Build the PRIMARY KEYS list divided by ENTITY
                                If Not DBSchema.Rows(i).IsNull("IsKey") AndAlso DBSchema.Rows(i)("IsKey") = True Then
                                    If Not PrimaryKeys.ContainsKey(ColumnEntityName) Then PrimaryKeys.Add(ColumnEntityName, New Dictionary(Of String, Object))
                                    PrimaryKeys(ColumnEntityName).Add(Reader.GetName(i), Reader(i))
                                End If

                                If Not String.IsNullOrWhiteSpace(Me.MainEntity) AndAlso Me.MainEntity.ToLower() <> ColumnEntityName.ToLower() Then
                                    If Not Record.Fields.ContainsKey(ColumnEntityName) Then Record.Fields.Add(ColumnEntityName, New Dictionary(Of String, Object))
                                    DirectCast(Record.Fields(ColumnEntityName), Dictionary(Of String, Object)).Add(Reader.GetName(i), If(Reader.IsDBNull(i), Nothing, Reader(i)))
                                Else
                                    Record.Fields.Add(Reader.GetName(i), If(Reader.IsDBNull(i), Nothing, Reader(i)))
                                End If

                                If Not String.IsNullOrWhiteSpace(EntityFieldID) Then
                                    Record.Id = Reader(EntityFieldID)
                                End If

                                If Not String.IsNullOrWhiteSpace(EntityFieldName) Then
                                    Record.Name = Reader(EntityFieldName)
                                End If

                                ''Retrieve NAME By Convention (PrimaryKey Column Name + "Name"), but with the possibility to change the convention (with prefix and suffix too)

                            Next
                            Record.Schema.PrimaryKeys = New ReadOnlyDictionary(Of String, Object)(PrimaryKeys)

                            'Dim EventArgs As New DynamoMappingDataToEntityEventArgs
                            'EventArgs.DataSchema = DBSchema
                            'EventArgs.Entity = Record

                            If HaveToAddNewEntity Then Result.Add(Record)
                        End While
                    End Using
                    Command.Parameters.Clear()
                End Using
            End Using
        End With

        Return Result
    End Function

    Private Function CheckIfEntityAlreadyExist(ByVal EntityName As String, ByRef QueryPartialResult As List(Of Dynamo.Entities.Entity), ByRef Reader As SqlDataReader) As Dynamo.Entities.Entity
        For Each e In From SingleEntity In QueryPartialResult Select New With {.Entity = SingleEntity, .PrimaryKeys = SingleEntity.Schema.PrimaryKeys(EntityName)}
            Dim AlreadyExistEntity As Boolean = True
            For Each PrimayKey As KeyValuePair(Of String, Object) In e.PrimaryKeys
                If ((Reader(PrimayKey.Key) Is DBNull.Value AndAlso PrimayKey.Value Is Nothing) OrElse (PrimayKey.Value = Reader(PrimayKey.Key))) Then
                    AlreadyExistEntity *= True
                Else
                    AlreadyExistEntity *= False
                End If
            Next

            If AlreadyExistEntity Then
                Return e.Entity
            End If
        Next

        Return Nothing
    End Function

    Public Overrides Function SortBy(EntityAlias As String, FieldName As String, Direction As SortDirections) As IQueryBuilder
        If SortBuilder.Length <= 0 Then
            SortBuilder.Append(" ORDER BY ")
            SortBuilder.AppendFormat("[{0}].[{1}] {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        Else
            TempBuilder.Clear()
            TempBuilder.AppendFormat("[{0}].[{1}]", EntityAlias, FieldName)
            If SortBuilder.ToString().Contains(TempBuilder.ToString) Then
                Throw New Exception(String.Format("The field {0}.{1} is already used in ORDER BY expression!", EntityAlias, FieldName))
            End If
            SortBuilder.AppendFormat(", [{0}].[{1}] {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        End If
        Return Me
    End Function

    Public Overrides Function OrFilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" OR [{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])
        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpression As Expressions.DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" OR [{0}].[{1}] {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value, FilterExpression.Operator)
        Return Me
    End Function

    Public Overloads Overrides Function FilterBy(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        Else
            FilterBuilder.Append(" AND (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        End If

        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        Else
            FilterBuilder.Append(" OR (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        End If

        Return Me
    End Function

    Public Overrides Function By(ByRef RelationshipExpessions As IEnumerable(Of DynamoRelationshipExpression)) As IQueryBuilder
        For Each Expression In RelationshipExpessions
            EntitiesRelationships.Add(New EntitiesRelationship With {.MasterEntity = EntitiesAlias(Expression.RelatedEntityAlias), .SlaveEntity = EntitiesAlias(TempBuilder.ToString)})
            JoinBuilder.AppendFormat(" [{0}].[{1}] {2} [{3}].[{4}]", TempBuilder.ToString, Expression.FieldName, FilterOperatorToString(Expression.Operator), Expression.RelatedEntityAlias, Expression.RelatedFieldName)
        Next
        Return Me
    End Function

    Public Overrides Function By(FieldName As String, JoinOperator As RelationshipOperators, RelatedEntityAlias As String, RelatedFieldName As String) As IQueryBuilder
        EntitiesRelationships.Add(New EntitiesRelationship With {.MasterEntity = EntitiesAlias(RelatedEntityAlias), .SlaveEntity = EntitiesAlias(TempBuilder.ToString)})
        JoinBuilder.AppendFormat(" [{0}].[{1}] {2} [{3}].[{4}]", TempBuilder.ToString, FieldName, FilterOperatorToString(JoinOperator), RelatedEntityAlias, RelatedFieldName)
        Return Me
    End Function

    Public Overrides Function Join(EntityName As String, EntityAlias As String, IsEntityRequried As Boolean) As IRelationshipQueryBuilder
        If EntitiesAlias.ContainsKey(EntityAlias) Then Throw New Exception("The entity alias " + EntityAlias + " is already used!")
        EntitiesAlias.Add(EntityAlias, EntityName)

        JoinBuilder.AppendFormat(" {0} JOIN [{1}] [{2}] ON", If(IsEntityRequried, "INNER", "LEFT"), EntityName, EntityAlias)
        TempBuilder.Clear()
        TempBuilder.Append(EntityAlias)
        Return Me
    End Function
End Class

Friend Class EntitiesRelationship
    Public Property MasterEntity
    Public Property SlaveEntity
End Class