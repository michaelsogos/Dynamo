Imports System.Linq.Expressions
Imports Dynamo.Entities

Public Class DynamoQueryable
    Implements IOrderedQueryable
    Implements IOrderedQueryable(Of Entity)

    Private _Provider As DynamoProvider
    Private _Expression As Expression
    Public EntityName As String

    Public Sub New(ByVal EntityName As String, ByVal Provider As DynamoProvider)
        Me._Provider = Provider
        Me._Expression = Expressions.Expression.Constant(Me)
        Me.EntityName = EntityName
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

    Public Function GetEnumerator() As IEnumerator(Of Entity) Implements IEnumerable(Of Entity).GetEnumerator
        Return _Provider.Execute(Of IEnumerable(Of Entity))(Expression).GetEnumerator()
    End Function
End Class
