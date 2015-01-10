Imports System.Text
Imports Dynamo.Contracts
Imports System.Data.SqlClient
Imports Dynamo.Entities
Imports System.Collections.ObjectModel

Public Class SQLQueryBuilder
    Inherits DynamoQueryBuilder

    Private Const GetForeignKeysQuery As String = "SELECT OBJECT_NAME(f.constraint_object_id) AS 'ForeignKey', OBJECT_NAME(f.parent_object_id) AS 'FKTable', c1.[name] AS 'FKColumnName', OBJECT_NAME(f.referenced_object_id) AS 'PKTable', c2.[name] AS 'PKColumnName' FROM sys.foreign_key_columns f INNER JOIN sys.all_columns c1 ON f.parent_object_id = c1.[object_id] AND f.parent_column_id = c1.column_id INNER JOIN sys.all_columns c2 ON f.referenced_object_id = c2.[object_id] AND f.referenced_column_id = c2.column_id WHERE OBJECT_NAME(f.parent_object_id) = @EntityName"

    Private EntitiesAlias As Dictionary(Of String, String)
    Private TempBuilder As StringBuilder
    Private ParamNameBuilder As StringBuilder
    Private QueryBuilder As StringBuilder
    Private FilterBuilder As StringBuilder
    Private SortBuilder As StringBuilder
    Private Parameters As List(Of SqlParameter)

    Public Sub New(ByRef Repository As SQLRepository, ByVal EntityName As String)
        MyBase.New(Repository, EntityName)

        EntitiesAlias = New Dictionary(Of String, String)
        Parameters = New List(Of SqlParameter)
        TempBuilder = New StringBuilder
        ParamNameBuilder = New StringBuilder
        QueryBuilder = New StringBuilder
        FilterBuilder = New StringBuilder
        SortBuilder = New StringBuilder
        QueryBuilder.AppendFormat("SELECT * FROM {0} t0", EntityName)
        EntitiesAlias.Add("t0", EntityName)
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
            FilterBuilder.AppendFormat("t0.[{0}] {1} ", Expression.FieldName, FilterOperatorToString(Expression.Operator))
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

    Public Overrides Function FilterBy(FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE ")
            FilterBuilder.AppendFormat("t0.[{0}] {1} ", FieldName, FilterOperatorToString([Operator]))
        Else
            FilterBuilder.AppendFormat(" AND t0.[{0}] {1} ", FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])
        Return Me
    End Function

    Public Overrides Function FilterBy(ByRef FilterExpression As Expressions.DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE ")
            FilterBuilder.AppendFormat("t0.[{0}] {1} ", FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        Else
            FilterBuilder.AppendFormat(" AND t0.[{0}] {1} ", FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value, FilterExpression.Operator)
        Return Me
    End Function

    Public Overrides Function Execute() As List(Of Entity)
        Dim Result As New List(Of Entity)
        Dim EntityFieldID As String = Nothing
        Dim EntityFieldName As String = Nothing

        TempBuilder.Clear()
        TempBuilder.Append(QueryBuilder).Append(FilterBuilder.ToString).Append(SortBuilder.ToString)

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
                        If Not EntitiesRelationships.ContainsKey(EntityName) Then
                            Using DBCommForeignKeys As New SqlCommand(GetForeignKeysQuery, .Connection)
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
                            End Using
                        End If
                        'retrieve query result
                        While Reader.Read
                            Dim Record As New Entity
                            Record.Schema.EntityObjectName = EntityName
                            Dim PrimaryKeys As New Dictionary(Of String, Object)
                            For i As Integer = 0 To Reader.FieldCount - 1
                                Record.Fields.Add(Reader.GetName(i), If(Reader.IsDBNull(i), Nothing, Reader(i)))

                                If Not DBSchema.Rows(i).IsNull("IsKey") AndAlso DBSchema.Rows(i)("IsKey") = True Then
                                    PrimaryKeys.Add(Reader.GetName(i), Reader(i))
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

                            Dim EventArgs As New DynamoMappingDataToEntityEventArgs
                            EventArgs.DataSchema = DBSchema
                            EventArgs.Entity = Record
                            Result.Add(Record)
                        End While
                    End Using
                    Command.Parameters.Clear()
                End Using
            End Using
        End With

        Return Result
    End Function

    Public Overrides Function SortBy(FieldName As String, Direction As SortDirections) As IQueryBuilder
        If SortBuilder.Length <= 0 Then
            SortBuilder.Append(" ORDER BY ")
            SortBuilder.AppendFormat("t0.[{0}] {1} ", FieldName, SortDirectionToString(Direction))
        Else
            TempBuilder.Clear()
            TempBuilder.AppendFormat("t0.[{0}]", FieldName)
            If SortBuilder.ToString().Contains(TempBuilder.ToString) Then
                Throw New Exception(String.Format("The field {0} is already used in ORDER BY expression!", FieldName))
            End If
            SortBuilder.AppendFormat(", t0.[{0}] {1} ", FieldName, SortDirectionToString(Direction))
        End If
        Return Me
    End Function

    Public Overrides Function OrFilterBy(FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" OR t0.[{0}] {1} ", FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])
        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpression As Expressions.DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" OR t0.[{0}] {1} ", FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
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
End Class