$(function ()
{
    $("img#sourceplot_0").load(function(){$("div#lightcurve").css('height', $(this).height())});
    $("img.thumbnail").hover(
        function ()
	{
	    $("img#sourceplot_" + $(this).attr("number")).css('display', 'block');
	},
	function ()
	{
	    $("img#sourceplot_" + $(this).attr("number")).css('display', 'none');
	});
	
	$("img.thumbnail").click(
		function ()
		{
			if($(this).attr("clicked")=="0"){
				$(this).attr("height","100%");
				$(this).attr("clicked","1")
			}
			else{
				$(this).attr("height","20");
				$(this).attr("clicked","0")
			}
		}
	);
});
