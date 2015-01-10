Imports Dynamo.Entities
Imports Dynamo.Contracts
Imports Dynamo.Expressions

Namespace Contracts
    Public Interface IQueryBuilder
        Function FilterBy(ByVal FieldName As String, ByVal [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder
        Function FilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), ByVal Combiner As FilterCombiners) As IFilterQueryBuilder
        Function SortBy(ByVal FieldName As String, ByVal Direction As SortDirections) As IQueryBuilder
        Function Execute() As List(Of Entity)
    End Interface

    Public Interface IFilterQueryBuilder
        Inherits IQueryBuilder

        Function OrFilterBy(ByVal FieldName As String, ByVal [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder
        Function OrFilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder
        Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), ByVal Combiner As FilterCombiners) As IFilterQueryBuilder
    End Interface
End Namespace

Public MustInherit Class DynamoQueryBuilder
    Implements IQueryBuilder
    Implements IFilterQueryBuilder

    Private _EntityName As String
    Private _Repository As IRepository

    Public ReadOnly Property EntityName As String
        Get
            Return _EntityName
        End Get
    End Property

    Public ReadOnly Property Repository As IRepository
        Get
            Return _Repository
        End Get
    End Property

    Protected ReadOnly Property EntitiesRelationships As Dictionary(Of String, List(Of EntityRelationShip))
        Get
            Return DynamoCache.EntitiesRelationships
        End Get
    End Property

    Public Sub New(ByRef Repository As IRepository, ByVal EntityName As String)
        Me._EntityName = EntityName
        Me._Repository = Repository
    End Sub

    ''' <summary>
    ''' Filter the result. The expression will be appended with AND operator if a previous expression already exist.
    ''' </summary>
    ''' <param name="FieldName"></param>
    ''' <param name="Operator"></param>
    ''' <param name="Value"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function FilterBy(FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    ''' <summary>
    ''' Filter the result. The expression will be appended with AND operator if a previous expression already exist.
    ''' </summary>
    ''' <param name="FilterExpression"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    ''' <summary>
    ''' Filter the result with a block of filter expressions combined by [Combiner] parameter. The block will be appended with AND operator if a previous expression already exist.
    ''' </summary>
    ''' <param name="FilterExpressions"></param>
    ''' <param name="Combiner"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function FilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    ''' <summary>
    ''' Filter the result. The expression will be appended with OR operator.
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function OrFilterBy(FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy

    ''' <summary>
    ''' Filter the result. The expression will be appended with OR operator.
    ''' </summary>
    ''' <param name="FilterExpression"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function OrFilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy

    ''' <summary>
    ''' Filter the result with a block of filter expression combined by [Combiner] paramter. The expression will be appended with OR operator.
    ''' </summary>
    ''' <param name="FilterExpressions"></param>
    ''' <param name="Combiner"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public MustOverride Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy

    Public MustOverride Function Execute() As List(Of Entity) Implements IQueryBuilder.Execute

    Public MustOverride Function SortBy(FieldName As String, Direction As SortDirections) As IQueryBuilder Implements IQueryBuilder.SortBy


End Class

<Flags>
Public Enum FilterOperators
    [Not] = 1
    Equal = 2
    GreaterThan = 4
    LessThan = 8
    Pattern = 16
    Include = 32
End Enum

Public Enum SortDirections
    Ascending = 0
    Descending = 1
End Enum

Public Enum FilterCombiners
    [And] = 0
    [Or] = 1
End Enum

Namespace Expressions
    Public Class DynamoFilterExpression

        Private _FieldName As String
        Private _Operator As FilterOperators
        Private _Value As Object

        Public ReadOnly Property FieldName As String
            Get
                Return _FieldName
            End Get
        End Property
        Public ReadOnly Property [Operator] As FilterOperators
            Get
                Return _Operator
            End Get
        End Property
        Public ReadOnly Property Value As Object
            Get
                Return _Value
            End Get
        End Property

        Public Sub New(FieldName As String, [Operator] As FilterOperators, Value As Object)
            _FieldName = FieldName
            _Operator = [Operator]
            _Value = Value
        End Sub
    End Class
End Namespace