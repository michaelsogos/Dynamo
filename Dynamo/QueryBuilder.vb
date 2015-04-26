Imports Dynamo.Entities
Imports Dynamo.Contracts
Imports Dynamo.Expressions

Namespace Contracts
    Public Interface IQueryBuilder
        ''' <summary>
        ''' Filter the result. The expression will be appended with AND operator if a previous expression already exist.
        ''' </summary>
        ''' <param name="FieldName"></param>
        ''' <param name="Operator"></param>
        ''' <param name="Value"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function FilterBy(ByVal EntityAlias As String, ByVal FieldName As String, ByVal [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder

        ''' <summary>
        ''' Filter the result. The expression will be appended with AND operator if a previous expression already exist.
        ''' </summary>
        ''' <param name="FilterExpression"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder

        ''' <summary>
        ''' Filter the result with a block of filter expressions combined by [Combiner] parameter. The block will be appended with AND operator if a previous expression already exist.
        ''' </summary>
        ''' <param name="FilterExpressions"></param>
        ''' <param name="Combiner"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function FilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), ByVal Combiner As FilterCombiners) As IFilterQueryBuilder

        Function SortBy(ByVal EntityAlias As String, ByVal FieldName As String, ByVal Direction As SortDirections) As IQueryBuilder

        ''' <summary>
        ''' Join the specified entity data type.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function Join(ByVal EntityName As String, ByVal EntityAlias As String, IsEntityRequried As Boolean) As IRelationshipQueryBuilder

        Function Execute() As List(Of Entity)
    End Interface

    Public Interface IFilterQueryBuilder
        Inherits IQueryBuilder

        ''' <summary>
        ''' Filter the result. The expression will be appended with OR operator.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function OrFilterBy(ByVal EntityAlias As String, ByVal FieldName As String, ByVal [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder

        ''' <summary>
        ''' Filter the result. The expression will be appended with OR operator.
        ''' </summary>
        ''' <param name="FilterExpression"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function OrFilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder

        ''' <summary>
        ''' Filter the result with a block of filter expression combined by [Combiner] paramter. The expression will be appended with OR operator.
        ''' </summary>
        ''' <param name="FilterExpressions"></param>
        ''' <param name="Combiner"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), ByVal Combiner As FilterCombiners) As IFilterQueryBuilder
    End Interface

    Public Interface IRelationshipQueryBuilder
        ''' <summary>
        ''' Specify join rules.
        ''' </summary>
        ''' <param name="RelationshipExpessions"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function By(ByRef RelationshipExpessions As IEnumerable(Of DynamoRelationshipExpression)) As IQueryBuilder

        ''' <summary>
        ''' Specify a single join rule
        ''' </summary>
        ''' <param name="FieldName"></param>
        ''' <param name="JoinOperator"></param>
        ''' <param name="RelatedEntityAlias"></param>
        ''' <param name="RelatedFieldName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Function By(ByVal FieldName As String, ByVal [JoinOperator] As RelationshipOperators, ByVal RelatedEntityAlias As String, ByVal RelatedFieldName As String) As IQueryBuilder
    End Interface
End Namespace

Public MustInherit Class DynamoQueryBuilder
    Implements IQueryBuilder
    Implements IFilterQueryBuilder
    Implements IRelationshipQueryBuilder

    Private _Repository As IRepository
    Private _Entities As Dictionary(Of String, String)

    Public ReadOnly Property Repository As IRepository
        Get
            Return _Repository
        End Get
    End Property
    Public ReadOnly Property Entities As Dictionary(Of String, String)
        Get
            Return _Entities
        End Get
    End Property

    Protected ReadOnly Property EntitiesForeignkeys As Dictionary(Of String, List(Of EntityForeignkey))
        Get
            Return DynamoCache.EntitiesForeignkeys
        End Get
    End Property
    Protected ReadOnly MainEntity As String

    Public Sub New(ByRef Repository As IRepository, ByVal EntityName As String, ByVal EntityAlias As String)
        _Repository = Repository
        _Entities = New Dictionary(Of String, String)
        _Entities.Add(EntityAlias, EntityName)
        MainEntity = EntityName
    End Sub

    Public MustOverride Function Execute() As List(Of Entity) Implements IQueryBuilder.Execute

#Region "FilterMethods"
    Public MustOverride Function FilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    Public MustOverride Function FilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    Public MustOverride Function FilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder Implements IQueryBuilder.FilterBy

    Public MustOverride Function OrFilterBy(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, ByRef Value As Object) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy

    Public MustOverride Function OrFilterBy(ByRef FilterExpression As DynamoFilterExpression) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy

    Public MustOverride Function OrFilterBy(ByRef FilterExpressions As IEnumerable(Of DynamoFilterExpression), Combiner As FilterCombiners) As IFilterQueryBuilder Implements IFilterQueryBuilder.OrFilterBy
#End Region

#Region "SortMethods"
    Public MustOverride Function SortBy(EntityAlias As String, FieldName As String, Direction As SortDirections) As IQueryBuilder Implements IQueryBuilder.SortBy
#End Region

#Region "JoinMethods"
    Public MustOverride Function Join(EntityName As String, EntityAlias As String, IsEntityRequried As Boolean) As IRelationshipQueryBuilder Implements IQueryBuilder.Join
    Public MustOverride Function By(ByRef RelationshipExpessions As IEnumerable(Of DynamoRelationshipExpression)) As IQueryBuilder Implements IRelationshipQueryBuilder.By
    Public MustOverride Function By(FieldName As String, JoinOperator As RelationshipOperators, RelatedEntityAlias As String, RelatedFieldName As String) As IQueryBuilder Implements IRelationshipQueryBuilder.By
#End Region

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

<Flags>
Public Enum RelationshipOperators
    [Not] = 1
    Equal = 2
    GreaterThan = 4
    LessThan = 8
End Enum

Namespace Expressions
    Public Class DynamoFilterExpression
        Private _FieldName As String
        Private _Operator As FilterOperators
        Private _Value As Object
        Private _EntityAlias As String

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
        Public ReadOnly Property EntityAlias As String
            Get
                Return _EntityAlias
            End Get
        End Property

        Public Sub New(EntityAlias As String, FieldName As String, [Operator] As FilterOperators, Value As Object)
            _FieldName = FieldName
            _Operator = [Operator]
            _Value = Value
            _EntityAlias = EntityAlias
        End Sub
    End Class

    Public Class DynamoRelationshipExpression
        Private _RelatedEntityAlias As String
        Private _RelatedFieldName As String
        Private _Operator As RelationshipOperators
        Private _FieldName As String

        Public ReadOnly Property RelatedEntityAlias As String
            Get
                Return _RelatedEntityAlias
            End Get
        End Property
        Public ReadOnly Property RelatedFieldName As String
            Get
                Return _RelatedFieldName
            End Get
        End Property
        Public ReadOnly Property [Operator] As RelationshipOperators
            Get
                Return _Operator
            End Get
        End Property
        Public ReadOnly Property FieldName As String
            Get
                Return _FieldName
            End Get
        End Property

        Public Sub New(RelatedEntityAlias As String, RelatedFieldName As String, [Operator] As RelationshipOperators, FieldName As String)
            _RelatedEntityAlias = RelatedEntityAlias
            _RelatedFieldName = RelatedFieldName
            _Operator = [Operator]
            _FieldName = FieldName
        End Sub
    End Class
End Namespace