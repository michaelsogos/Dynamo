Imports System.Text
Imports Dynamo.Contracts
Imports System.Data.SqlClient
Imports Dynamo.Entities
Imports System.Collections.ObjectModel
Imports Dynamo.Expressions
Imports System.Reflection
Imports System.Data.Common

Public Class QueryBuilder
    Inherits DynamoQueryBuilder

#Region "Class Variables"
    'Private Const GetForeignKeysQuery As String = "SELECT OBJECT_NAME(f.constraint_object_id) AS 'ForeignKey', OBJECT_NAME(f.parent_object_id) AS 'FKTable', c1.[name] AS 'FKColumnName', OBJECT_NAME(f.referenced_object_id) AS 'PKTable', c2.[name] AS 'PKColumnName' FROM sys.foreign_key_columns f INNER JOIN sys.all_columns c1 ON f.parent_object_id = c1.[object_id] AND f.parent_column_id = c1.column_id INNER JOIN sys.all_columns c2 ON f.referenced_object_id = c2.[object_id] AND f.referenced_column_id = c2.column_id WHERE OBJECT_NAME(f.parent_object_id) = @EntityName"

    Private TempBuilder As StringBuilder
    Private ParamNameBuilder As StringBuilder
    Private FilterBuilder As StringBuilder
    Private SortBuilder As StringBuilder
    Private Parameters As List(Of SqlParameter)
    Private MARSQueries As Dictionary(Of String, String)
    Private EntityDependencies As Dictionary(Of String, HashSet(Of String))
    Private ElaboratedDependeciesAlias As HashSet(Of String)
    Private NestedExpressions As List(Of NestedExpression)
    Private LatestMethodParameters As Dictionary(Of String, Object)
    Private DBResult As DataSet


#End Region

    Public Sub New(ByRef Repository As Repository, ByVal EntityName As String, ByVal EntityAlias As String)
        MyBase.New(Repository, EntityName, EntityAlias)

        Parameters = New List(Of SqlParameter)
        TempBuilder = New StringBuilder
        ParamNameBuilder = New StringBuilder
        FilterBuilder = New StringBuilder
        SortBuilder = New StringBuilder
        MARSQueries = New Dictionary(Of String, String)(StringComparer.InvariantCultureIgnoreCase)
        EntityDependencies = New Dictionary(Of String, HashSet(Of String))(StringComparer.InvariantCultureIgnoreCase)
        ElaboratedDependeciesAlias = New HashSet(Of String)(StringComparer.InvariantCultureIgnoreCase)
        NestedExpressions = New List(Of NestedExpression)
        LatestMethodParameters = New Dictionary(Of String, Object)
        DBResult = New DataSet

        MARSQueries.Add(EntityAlias.ToLower, String.Format("SELECT {1}.* FROM [{0}] {1}", EntityName, EntityAlias))
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
            If (Expression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, Expression.EntityAlias)

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

    Private Sub SetEntityDependency(ByVal NestedEntityAlias As String, ByVal ParentEntityAlias As String)
        If Not EntityDependencies.ContainsKey(NestedEntityAlias) Then EntityDependencies.Add(NestedEntityAlias, New HashSet(Of String)(StringComparer.InvariantCultureIgnoreCase))
        EntityDependencies(NestedEntityAlias).Add(ParentEntityAlias)
    End Sub

    Public Overrides Function FilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE ")
            FilterBuilder.AppendFormat("[{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        Else
            FilterBuilder.AppendFormat(" AND [{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])

        If (EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, EntityAlias)

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

        If (FilterExpression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, FilterExpression.EntityAlias)

        Return Me
    End Function

    Public Overrides Function Execute() As List(Of Entity)
        Dim Result As New List(Of Entity)

        TempBuilder.Clear()
        For Each SqlQuery In MARSQueries
            TempBuilder.Append(SqlQuery.Value)

            ElaborateDependencies(SqlQuery.Key, SqlQuery.Key)
            ElaboratedDependeciesAlias.Clear()
            TempBuilder.Append(FilterBuilder.ToString)
            TempBuilder.Append(Environment.NewLine)
        Next

        With DirectCast(Repository, Repository)
            .OpenConnection()
            Using .Connection

                Using Command As New SqlCommand(TempBuilder.ToString, .Connection)
                    Command.Parameters.AddRange(Parameters.ToArray)

                    Dim Adapter As New SqlDataAdapter(Command)
                    Adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey
                    Adapter.Fill(DBResult)


                End Using
            End Using

            Dim Counter As Integer = 0
            For Each TableName In MARSQueries.Keys
                DBResult.Tables(Counter).TableName = TableName
                Counter += 1
            Next

            ConvertDBResultToObjectTree(Result)
        End With

        CleanParameters()
        Return Result
    End Function

    Private Sub ElaborateDependencies(ByVal NestedEntityAlias As String, ByVal QueryEntityAlias As String)
        If EntityDependencies.ContainsKey(NestedEntityAlias) Then
            For Each Dependency In EntityDependencies(NestedEntityAlias)
                If Dependency.ToLower = QueryEntityAlias.ToLower Then Continue For
                If ElaboratedDependeciesAlias.Contains(Dependency) Then Continue For

                Dim Expression = (From Item In NestedExpressions
                                  Where ((Item.ParentEntityAlias.ToLower = NestedEntityAlias.ToLower AndAlso Item.NestedEntityAlias.ToLower = Dependency.ToLower) _
                                  OrElse (Item.NestedEntityAlias.ToLower = NestedEntityAlias.ToLower AndAlso Item.ParentEntityAlias.ToLower = Dependency.ToLower))
                                  Select Item).FirstOrDefault()

                If Expression IsNot Nothing Then
                    For Each FieldMap In Expression.MatchingFieldsMap
                        If Expression.ParentEntityAlias.ToLower = Dependency.ToLower Then
                            TempBuilder.AppendFormat(" INNER JOIN [{0}] [{1}] ON [{1}].[{2}] = [{3}].[{4}]", Entities(Dependency), Dependency, FieldMap.ParentEntityFieldName, NestedEntityAlias, FieldMap.NestedEntityFieldName)
                        Else
                            TempBuilder.AppendFormat(" INNER JOIN [{0}] [{1}] ON [{1}].[{2}] = [{3}].[{4}]", Entities(Dependency), Dependency, FieldMap.NestedEntityFieldName, NestedEntityAlias, FieldMap.ParentEntityFieldName)
                        End If
                    Next
                End If

                ElaboratedDependeciesAlias.Add(NestedEntityAlias)
                If Not ElaboratedDependeciesAlias.Contains(Dependency) Then ElaborateDependencies(Dependency, QueryEntityAlias)
            Next
        End If
    End Sub

    Private Sub CleanParameters()
        'Clean because any subsequent query will be affected by these parameters
        EntityDependencies.Clear()
        Parameters.Clear()
        MARSQueries.Clear()
        NestedExpressions.Clear()
        LatestMethodParameters.Clear()
        ElaboratedDependeciesAlias.Clear()
    End Sub

    Private Sub ConvertDBResultToObjectTree(ByRef Result As List(Of Entity))
        Dim Table = DBResult.Tables(0)
        For Each Row As DataRow In Table.Rows
            Dim Record = New Entity
            Record.Schema.EntityName = Entities(Table.TableName)

            RetrieveEntitySchema(Row, Record)
            ConvertColumnsToEntityFields(Row, Record)
            CreateNestedObjects(Table.TableName, Record)

            Result.Add(Record)
        Next
    End Sub

    Private Sub RetrieveEntitySchema(ByRef Row As DataRow, ByRef Record As Entity)
        With DirectCast(Repository, Repository)
            If .Conventions.AutodetectEntityFieldID Then
                If Row.Table.PrimaryKey.Count = 1 Then
                    Record.Schema.FieldID = Row.Table.PrimaryKey.FirstOrDefault.ColumnName
                    Record.Id = Row(Row.Table.PrimaryKey.FirstOrDefault.ColumnName)
                Else
                    Record.Schema.Errors.Add("Cannot retrieve the PRIMARY KEY from the entity schema because it is empty or too many primary key is defined.")
                    RetrieveSchemaFieldID(Row, Record)
                End If
            Else
                RetrieveSchemaFieldID(Row, Record)
            End If

            RetrieveSchemaFieldName(Row, Record)
        End With
    End Sub

    Private Sub RetrieveSchemaFieldID(ByRef Row As DataRow, ByRef Record As Entity)
        Dim LookupNames = DirectCast(Repository, Repository).Conventions.EntityFieldID.Replace("{entityname}", Record.Schema.EntityName).ToLower().Split("|")
        Dim FieldID = (From Column As DataColumn In Row.Table.Columns Where LookupNames.Contains(Column.ColumnName.ToLower) Select Column.ColumnName).FirstOrDefault
        If Not String.IsNullOrWhiteSpace(FieldID) Then
            Record.Schema.FieldID = FieldID
            Record.Id = Row(FieldID)
        Else
            Record.Schema.Errors.Add("Cannot retrieve the ENTITY ID by using convention.")
        End If
    End Sub

    Private Sub RetrieveSchemaFieldName(ByRef Row As DataRow, ByRef Record As Entity)
        Dim LookupNames = DirectCast(Repository, Repository).Conventions.EntityFieldName.Replace("{entityname}", Record.Schema.EntityName).ToLower().Split("|")
        Dim FieldName = (From Column As DataColumn In Row.Table.Columns Where LookupNames.Contains(Column.ColumnName.ToLower) Select Column.ColumnName).FirstOrDefault
        If Not String.IsNullOrWhiteSpace(FieldName) Then
            Record.Schema.FieldName = FieldName
            Record.Name = Row(FieldName)
        Else
            Record.Schema.Errors.Add("Cannot retrieve the ENTITY NAME by using convention.")
        End If
    End Sub

    Private Sub ConvertColumnsToEntityFields(ByRef Row As DataRow, ByRef Record As Entity)
        For Each Column As DataColumn In Row.Table.Columns
            Record.Fields.Add(Column.ColumnName, Row(Column))
        Next
    End Sub

    Private Sub CreateNestedObjects(ByVal EntityAlias As String, ByRef Record As Entity)
        Dim NestedFilteredExpressions = (From Item In NestedExpressions Where Item.ParentEntityAlias.ToLower = EntityAlias.ToLower Select Item).ToList
        For Each NestedExpression In NestedFilteredExpressions
            Dim NestedEntities As New List(Of Entity)
            ConvertNestedTableToObjectTree(DBResult.Tables(NestedExpression.NestedEntityAlias), NestedEntities, NestedExpression, Record)
            Record.Fields.Add(NestedExpression.NestedEntityAlias, NestedEntities)
        Next
    End Sub

    Private Sub ConvertNestedTableToObjectTree(ByRef Table As DataTable, ByRef Result As List(Of Entity), ByRef Expression As NestedExpression, ByRef ParentEntity As Entity)
        Dim NestedComposableQuery = From Record As DataRow In Table.Rows
        For Each MatchFieldMap In Expression.MatchingFieldsMap
            Dim ParentValue = ParentEntity.Fields(MatchFieldMap.ParentEntityFieldName)
            NestedComposableQuery = From Record In NestedComposableQuery Where Record(MatchFieldMap.NestedEntityFieldName) = ParentValue
        Next
        For Each Row In NestedComposableQuery.ToList()
            Dim Record = New Entity
            Record.Schema.EntityName = Entities(Table.TableName)

            RetrieveEntitySchema(Row, Record)
            ConvertColumnsToEntityFields(Row, Record)
            CreateNestedObjects(Table.TableName, Record)

            Result.Add(Record)
        Next

    End Sub

    Public Overrides Function SortBy(EntityAlias As String, FieldName As String, Direction As SortDirections) As IQueryBuilder
        If SortBuilder.Length <= 0 Then
            SortBuilder.Append(" ORDER BY ")
            SortBuilder.AppendFormat("[{0}].[{1}] {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        Else
            TempBuilder.Clear()
            TempBuilder.AppendFormat("[{0}].[{1}]", EntityAlias, FieldName)
            If SortBuilder.ToString().Contains(TempBuilder.ToString) Then
                Throw New Exception(String.Format("The field {0}.{1} Is already used In ORDER BY expression!", EntityAlias, FieldName))
            End If
            SortBuilder.AppendFormat(", [{0}].[{1}] {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        End If
        Return Me
    End Function

    Public Overrides Function OrFilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" Or [{0}].[{1}] {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value, [Operator])

        If (EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, EntityAlias)

        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpression As Expressions.DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" Or [{0}].[{1}] {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value, FilterExpression.Operator)

        If (FilterExpression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, FilterExpression.EntityAlias)

        Return Me
    End Function

    Public Overloads Overrides Function FilterBy(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" WHERE (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        Else
            FilterBuilder.Append(" And (")
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
            FilterBuilder.Append(" Or (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        End If

        Return Me
    End Function

    Public Overrides Function WithNested(NestedEntityName As String, NestedEntityAlias As String, ParentEntityAlias As String) As INestedQueryBuilder
        MARSQueries.Add(NestedEntityAlias.ToLower, String.Format("Select {1}.* FROM [{0}] {1}", NestedEntityName, NestedEntityAlias))

        NestedExpressions.Add(New NestedExpression(ParentEntityAlias, NestedEntityAlias))

        SetEntityDependency(NestedEntityAlias, ParentEntityAlias)

        Entities.Add(NestedEntityAlias, NestedEntityName)

        LatestMethodParameters.Clear()
        LatestMethodParameters.Add("NestedEntityName", NestedEntityName)
        LatestMethodParameters.Add("NestedEntityAlias", NestedEntityAlias)
        LatestMethodParameters.Add("ParentEntityAlias", ParentEntityAlias)

        Return Me
    End Function

    Public Overrides Function By(NestedEntityFieldName As String, ParentEntityFieldName As String) As INestedQueryBuilder
        If Not LatestMethodParameters.ContainsKey("NestedEntityAlias") Then Throw New NotSupportedException("The [By] method must be Call after [WithNested] method!")
        If Not LatestMethodParameters.ContainsKey("ParentEntityAlias") Then Throw New NotSupportedException("The [By] method must be Call after [WithNested] method!")

        Dim Expression = (From Item In NestedExpressions
                          Where Item.ParentEntityAlias.ToLower = LatestMethodParameters("ParentEntityAlias").ToLower() _
                              AndAlso Item.NestedEntityAlias.ToLower = LatestMethodParameters("NestedEntityAlias").ToLower()
                          Select Item).FirstOrDefault

        Expression.MatchingFieldsMap.Add(New NestedMatchingFieldsMap(ParentEntityFieldName, NestedEntityFieldName))

        Return Me
    End Function
End Class

Friend Class NestedExpression
    Public ReadOnly Property ParentEntityAlias As String
    Public ReadOnly Property NestedEntityAlias As String
    Public Property MatchingFieldsMap As List(Of NestedMatchingFieldsMap)

    Public Sub New(ByVal ParentEntityAlias As String, ByVal NestedEntityAlias As String)
        Me.ParentEntityAlias = ParentEntityAlias
        Me.NestedEntityAlias = NestedEntityAlias
        MatchingFieldsMap = New List(Of NestedMatchingFieldsMap)
    End Sub
End Class

Friend Class NestedMatchingFieldsMap
    Public ReadOnly Property ParentEntityFieldName As String
    Public ReadOnly Property NestedEntityFieldName As String

    Public Sub New(ByVal ParentEntityFieldName As String, ByVal NestedEntityFieldName As String)
        Me.ParentEntityFieldName = ParentEntityFieldName
        Me.NestedEntityFieldName = NestedEntityFieldName
    End Sub
End Class