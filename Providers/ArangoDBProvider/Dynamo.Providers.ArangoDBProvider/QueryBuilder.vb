Imports System.Text
Imports Dynamo.Contracts
Imports Dynamo.Entities
Imports System.Collections.ObjectModel
Imports Dynamo.Expressions
Imports System.Reflection
Imports System.Data.Common
Imports Arango.Client

Public Class QueryBuilder
    Inherits DynamoQueryBuilder

#Region "Class Variables"
    Private TempBuilder As StringBuilder
    Private ParamNameBuilder As StringBuilder
    Private FilterBuilder As StringBuilder
    Private SortBuilder As StringBuilder
    Private EntityDependencies As Dictionary(Of String, HashSet(Of String))
    Private ElaboratedDependeciesAlias As HashSet(Of String)
    Private NestedExpressions As List(Of NestedExpression)
    Private LatestMethodParameters As Dictionary(Of String, Object)
    Private Parameters As Dictionary(Of String, Object)
    Private Queries As Dictionary(Of String, String)
    Private DBResult As Dictionary(Of String, AResult(Of List(Of Object)))
    Private FetchedOn As DateTimeOffset
    Private Swatch As Stopwatch

#End Region

#Region "FILTERING Functions"
    Private Function FilterOperatorToString([Operator] As FilterOperators) As String
        Select Case [Operator]
            Case FilterOperators.Equal
                Return "=="
            Case FilterOperators.Not + FilterOperators.Equal
                Return "!="
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

    Private Sub ConvertValueToParameter(ByRef Value As Object)
        Select Case True
            Case TypeOf Value Is String
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Integer, TypeOf Value Is Int32, TypeOf Value Is Int16, TypeOf Value Is Int64, TypeOf Value Is UInteger, TypeOf Value Is UInt16, TypeOf Value Is UInt32, TypeOf Value Is UInt64 _
                 , TypeOf Value Is Short, TypeOf Value Is UShort, TypeOf Value Is Long, TypeOf Value Is ULong, TypeOf Value Is SByte, TypeOf Value Is Byte
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Decimal, TypeOf Value Is Double, TypeOf Value Is Single
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is Boolean
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is DateTime, TypeOf Value Is DateTimeOffset, TypeOf Value Is Date
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
            Case TypeOf Value Is IList
                'For performance reason the list cannot be greater than 2000 items
                Dim CastedValue = DirectCast(Value, IList)
                If CastedValue.Count > 2000 Then Throw New ArgumentException("The Value parameter cannot be a list greater than 2000 items!")
                Parameters.Add(GetNextParamName, Value)
                FilterBuilder.Append(ParamNameBuilder.ToString)
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
            ConvertValueToParameter(Expression.Value)
            If (Expression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, Expression.EntityAlias)

            If Counter < FilterExpressions.Count Then
                Select Case Combiner
                    Case FilterCombiners.And
                        FilterBuilder.Append(" && ")
                    Case FilterCombiners.Or
                        FilterBuilder.Append(" || ")
                    Case Else
                        Throw New NotImplementedException("FilterCombiner " + Combiner.ToString + " Not implemented!")
                End Select
            End If
        Next
    End Sub

    Public Overrides Function FilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" FILTER ")
            FilterBuilder.AppendFormat("`{0}`.`{1}` {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        Else
            FilterBuilder.AppendFormat(" && `{0}`.`{1}` {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value)

        If (EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, EntityAlias)

        Return Me
    End Function

    Public Overrides Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" FILTER ")
            FilterBuilder.AppendFormat("`{0}`.`{1}` {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        Else
            FilterBuilder.AppendFormat(" && `{0}`.`{1}` {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value)

        If (FilterExpression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, FilterExpression.EntityAlias)

        Return Me
    End Function

    Public Overrides Function OrFilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" || `{0}`.`{1}` {2} ", EntityAlias, FieldName, FilterOperatorToString([Operator]))
        End If

        ConvertValueToParameter(Value)

        If (EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, EntityAlias)

        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpression As Expressions.DynamoFilterExpression) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            Throw New Exception("Cannot execute this method before call FilterBy!")
        Else
            FilterBuilder.AppendFormat(" || `{0}`.`{1}` {2} ", FilterExpression.EntityAlias, FilterExpression.FieldName, FilterOperatorToString(FilterExpression.Operator))
        End If

        ConvertValueToParameter(FilterExpression.Value)

        If (FilterExpression.EntityAlias.ToLower <> MainEntityAlias.ToLower) Then SetEntityDependency(MainEntityAlias, FilterExpression.EntityAlias)

        Return Me
    End Function

    Public Overloads Overrides Function FilterBy(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" FILTER (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        Else
            FilterBuilder.Append(" && (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        End If

        Return Me
    End Function

    Public Overloads Overrides Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of Expressions.DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder
        If FilterBuilder.Length <= 0 Then
            FilterBuilder.Append(" FILTER (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        Else
            FilterBuilder.Append(" || (")
            ParseFilterExpressionList(FilterExpressions, Combiner)
            FilterBuilder.Append(")")
        End If

        Return Me
    End Function

#End Region

#Region "NESTING Functions"
    Private Sub SetEntityDependency(ByVal NestedEntityAlias As String, ByVal ParentEntityAlias As String)
        If Not EntityDependencies.ContainsKey(NestedEntityAlias) Then EntityDependencies.Add(NestedEntityAlias, New HashSet(Of String)(StringComparer.InvariantCultureIgnoreCase))
        EntityDependencies(NestedEntityAlias).Add(ParentEntityAlias)
    End Sub

    Public Overrides Function WithNested(NestedEntityName As String, NestedEntityAlias As String, ParentEntityAlias As String) As INestedQueryBuilder
        Queries.Add(NestedEntityAlias, String.Format("FOR `{1}` IN `{0}`", NestedEntityName, NestedEntityAlias))

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

#End Region

#Region "SORTING Functions"
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

    Public Overrides Function SortBy(EntityAlias As String, FieldName As String, Direction As SortDirections) As IQueryBuilder
        If SortBuilder.Length <= 0 Then
            SortBuilder.Append(" ORDER BY ")
            SortBuilder.AppendFormat("`{0}`.`{1}` {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        Else
            TempBuilder.Clear()
            TempBuilder.AppendFormat("`{0}`.`{1}`", EntityAlias, FieldName)
            If SortBuilder.ToString().Contains(TempBuilder.ToString) Then
                Throw New Exception(String.Format("The field {0}.{1} Is already used In ORDER BY expression!", EntityAlias, FieldName))
            End If
            SortBuilder.AppendFormat(", `{0}`.`{1}` {2} ", EntityAlias, FieldName, SortDirectionToString(Direction))
        End If
        Return Me
    End Function

#End Region

#Region "QUERY BUILDING Functions"
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
                    TempBuilder.AppendFormat(" FOR `{1}` IN `{0}`", Entities(Dependency), Dependency)
                    For Each FieldMap In Expression.MatchingFieldsMap
                        If Expression.ParentEntityAlias.ToLower = Dependency.ToLower Then
                            Dim TypedNestedFieldName = String.Format("`{0}`.`{1}`", NestedEntityAlias, FieldMap.NestedEntityFieldName)
                            If FieldMap.ParentEntityFieldName.ToLower = "_key" Then TypedNestedFieldName = String.Format("TO_STRING({0})", TypedNestedFieldName)
                            TempBuilder.AppendFormat(" FILTER `{0}`.`{1}` == {2}", Dependency, FieldMap.ParentEntityFieldName, TypedNestedFieldName)
                        Else
                            Dim TypedNestedFieldName = String.Format("`{0}`.`{1}`", Dependency, FieldMap.NestedEntityFieldName)
                            If FieldMap.ParentEntityFieldName.ToLower = "_key" Then TypedNestedFieldName = String.Format("TO_STRING({0})", TypedNestedFieldName)
                            TempBuilder.AppendFormat(" FILTER {0} == `{1}`.`{2}`", TypedNestedFieldName, NestedEntityAlias, FieldMap.ParentEntityFieldName)
                        End If
                    Next
                End If

                ElaboratedDependeciesAlias.Add(NestedEntityAlias)
                If Not ElaboratedDependeciesAlias.Contains(Dependency) Then ElaborateDependencies(Dependency, QueryEntityAlias)
            Next
        End If
    End Sub

#End Region

#Region "RESULT PARSING Functions"
    Private Sub ConvertDBResultToObjectTree(ByRef Result As List(Of Entity))
        Dim RootObject = DBResult(MainEntityAlias)
        For Each ArangoEntity As Dictionary(Of String, Object) In RootObject.Value
            Dim Record = DirectCast(Repository, Repository).ConvertResultToEntity(Entities(MainEntityAlias), ArangoEntity)
            DirectCast(Repository, Repository).RetrieveSchemaFieldName(ArangoEntity, Record)
            CreateNestedObjects(MainEntityAlias, Record)

            Record.Status.IssueTime = Swatch.Elapsed
            Record.Status.FetchedOn = FetchedOn

            Result.Add(Record)
        Next
    End Sub

    Private Sub CreateNestedObjects(ByVal EntityAlias As String, ByRef Record As Entity)
        Dim NestedFilteredExpressions = (From Item In NestedExpressions Where Item.ParentEntityAlias.ToLower = EntityAlias.ToLower Select Item).ToList
        For Each NestedExpression In NestedFilteredExpressions
            Dim NestedEntities As New List(Of Entity)
            ConvertNestedTableToObjectTree(DBResult(NestedExpression.NestedEntityAlias), NestedEntities, NestedExpression, Record)
            Record.Fields.Add(NestedExpression.NestedEntityAlias, NestedEntities)
        Next
    End Sub

    Private Sub ConvertNestedTableToObjectTree(ByRef Table As AResult(Of List(Of Object)), ByRef Result As List(Of Entity), ByRef Expression As NestedExpression, ByRef ParentEntity As Entity)
        Dim NestedComposableQuery = From Record In Table.Value
        For Each MatchFieldMap In Expression.MatchingFieldsMap
            Dim ParentValue = ParentEntity.Fields(MatchFieldMap.ParentEntityFieldName)
            NestedComposableQuery = From Record In NestedComposableQuery Where Record(MatchFieldMap.NestedEntityFieldName) = ParentValue
        Next
        For Each Row In NestedComposableQuery.ToList()
            Dim Record = DirectCast(Repository, Repository).ConvertResultToEntity(Entities(Expression.NestedEntityAlias), Row)
            DirectCast(Repository, Repository).RetrieveSchemaFieldName(Row, Record)
            CreateNestedObjects(Expression.NestedEntityAlias, Record)

            Result.Add(Record)
        Next
    End Sub

#End Region

    Public Sub New(ByRef Repository As Repository, ByVal EntityName As String, ByVal EntityAlias As String)
        MyBase.New(Repository, EntityName, EntityAlias)

        Queries = New Dictionary(Of String, String)(StringComparer.InvariantCultureIgnoreCase)
        TempBuilder = New StringBuilder
        ParamNameBuilder = New StringBuilder
        Parameters = New Dictionary(Of String, Object)
        FilterBuilder = New StringBuilder
        SortBuilder = New StringBuilder
        EntityDependencies = New Dictionary(Of String, HashSet(Of String))(StringComparer.InvariantCultureIgnoreCase)
        ElaboratedDependeciesAlias = New HashSet(Of String)(StringComparer.InvariantCultureIgnoreCase)
        NestedExpressions = New List(Of NestedExpression)
        LatestMethodParameters = New Dictionary(Of String, Object)
        DBResult = New Dictionary(Of String, AResult(Of List(Of Object)))
        Swatch = New Stopwatch

        Queries.Add(EntityAlias, String.Format("FOR `{1}` IN `{0}`", EntityName, EntityAlias))
    End Sub

    Public Overrides Function Execute() As List(Of Entity)
        Dim Result As New List(Of Entity)
        Dim AQLQueries As New Dictionary(Of String, String)

        For Each Query In Queries
            TempBuilder.Clear()
            TempBuilder.Append(Query.Value)

            ElaborateDependencies(Query.Key, Query.Key)
            ElaboratedDependeciesAlias.Clear()
            TempBuilder.Append(FilterBuilder.ToString)
            TempBuilder.AppendFormat(" RETURN `{0}`", Query.Key)
            AQLQueries.Add(Query.Key, TempBuilder.ToString)
        Next

        With DirectCast(Repository, Repository)
            .OpenConnection()

            FetchedOn = DateTimeOffset.UtcNow
            Swatch.Start()

            For Each AQLQuery In AQLQueries
                Dim QueryObject = .Context.Query.Aql(AQLQuery.Value)
                For Each Item In Parameters
                    QueryObject.BindVar(Item.Key.Replace("@", ""), Item.Value)
                Next
                DBResult.Add(AQLQuery.Key, QueryObject.ToList)
                If DBResult(AQLQuery.Key).Error IsNot Nothing Then Throw New Exception(DBResult(AQLQuery.Key).Error.Message)
            Next

            ConvertDBResultToObjectTree(Result)
        End With

        CleanParameters()
        Return Result
    End Function

    Private Sub CleanParameters()
        'Clean because any subsequent query will be affected by these parameters
        EntityDependencies.Clear()
        NestedExpressions.Clear()
        LatestMethodParameters.Clear()
        ElaboratedDependeciesAlias.Clear()
        Swatch.Start()
    End Sub

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