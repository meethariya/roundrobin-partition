from django.shortcuts import render
from . import store

# Create your views here.
def home(request):
    dictionary={}
    if request.method == 'POST':
        data = request.POST
        button = data.get('action')
        if button == "Partition":
            partitions = int(data.get('partitions'))
            dictionary = store.rrpartition(partitions)
        else:
            dictionary = store.reset_partition()
    # fetching all database info
    info = store.infor()
    # merging dictionary
    dictionary = dictionary|info
    return render(request, 'rrpartition/home.html',dictionary)