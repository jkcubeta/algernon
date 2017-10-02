function get_audit_targets() {
    $.ajax({
        type: 'POST',
        url: '/audit_targets',
        success: function(data, status, request){
            var ul = $("#targets")
            for(i = 0; i < data.length; i++){
                var target = document.createElement("li");
                target.appendChild(document.createTextNode(data[i]))
                ul.appendChild(target)
            }
        },
        error: function(){
            alert('things done broke')
        }
    })
}