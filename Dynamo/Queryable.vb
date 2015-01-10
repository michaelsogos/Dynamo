Imports System.Linq.Expressions
Imports Dynamo.Entities
Imports System.Runtime.CompilerServices

Public Class DynamoQueryable(Of TElement)
    Implements IQueryable
    Implements IQueryable(Of TElement)
    Implements IOrderedQueryable
    Implements IOrderedQueryable(Of TElement)

    Private _Provider As DynamoProvider
    Private _Expression As Expression
    Private _EntityName As String

    Public ReadOnly Property EntityName As String
        Get
            Return _EntityName
        End Get
    End Property

    Friend Sub New(ByVal Provider As DynamoProvider)
        Me._Provider = Provider
        Me._Expression = Expressions.Expression.Constant(me)
    End Sub

    Friend Sub New(ByVal Provider As DynamoProvider, ByVal QueryExpression As Expression)
        Me._Provider = Provider
        Me._Expression = QueryExpression
    End Sub

    Public ReadOnly Property ElementType As Type Implements IQueryable.ElementType
        Get
            Return GetType(Entity)
        End Get
    End Property

    Public ReadOnly Property Expression As Expressions.Expression Implements IQueryable.Expression
        Get
            Return _Expression
        End Get
    End Property

    Public ReadOnly Property Provider As IQueryProvider Implements IQueryable.Provider
        Get
            Return _Provider
        End Get
    End Property

    Public Function GetEnumerator1() As IEnumerator Implements IEnumerable.GetEnumerator
        Return _Provider.Execute(Expression).GetEnumerator()
    End Function

    Public Function GetEnumerator() As IEnumerator(Of TElement) Implements IEnumerable(Of TElement).GetEnumerator
        Return _Provider.Execute(Of IEnumerable(Of Entity))(Expression).GetEnumerator()
    End Function

    Protected Function Query(ByVal EntityName As String) As DynamoQueryable(Of TElement)
        _EntityName = EntityName
        Return Me
    End Function

End Class

Public Module DynamoQueryableExtensions
    <Extension>
    Public Function Query(Of TElement)(source As DynamoQueryable(Of TElement), ByVal EntityName As String) As DynamoQueryable(Of TElement)
        Return source.Provider.CreateQuery(Expression.Call(Expression.Constant(source), source.GetType().GetMethod("Query", Reflection.BindingFlags.Instance + Reflection.BindingFlags.NonPublic), Expression.Constant(EntityName)))
    End Function

End Module
