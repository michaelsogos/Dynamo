Namespace Collections
    Public Class Fields
        Inherits Dictionary(Of String, Object)

        Default Public Overloads Property Item(ByVal key As String) As Object
            Get
                Try
                    Return MyBase.Item(key)
                Catch ex As KeyNotFoundException
                    Throw New KeyNotFoundException(String.Format("Cannot find a field with name '{0}'. Remember that field name is case sensitive.", key))
                End Try
            End Get
            Set(ByVal value As Object)
                MyBase.Item(key) = value
            End Set
        End Property

        Public Sub AddRange(ByRef Dictionary As IDictionary(Of String, Object))
            For Each KeyValueItem In Dictionary
                Me(KeyValueItem.Key) = KeyValueItem.Value
            Next
        End Sub

    End Class
End Namespace

