from django import forms

class NameForm(forms.Form):
    keyword = forms.CharField(label='keyword', max_length= 25)

