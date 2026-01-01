package html

var cssText = `
<style type="text/css">

/* 默认样式 */
*{
font-size: 13px;
color: #2468a2;
}

h1 {
width: 100%;
display: block;
line-height: 1.5em;
overflow: visible;
font-size: 16px;
color: #2468a2;
}

h2 {
width: 100%;
display: block;
line-height: 1.5em;
overflow: visible;
font-size: 14px;
color: #2468a2;
}

/* 表格样式 */
table {
white-space: nowrap;
overflow: hidden;
text-overflow: ellipsis;
border-collapse: collapse;
margin-bottom: 1rem;
background-color: #fff;
color: #2468a2;
line-height: 1  ;
font-family: Arial, sans-serif;
}

table th,table td {
padding: 0.75rem;
vertical-align: top;
text-align: left;
border: 1px solid #dee2e6;
}

table th {
font-weight: bold;
background-color: #33a3dc;
color: #fff;
position: sticky;
top: 0;
}

/* 斑马线效果 */
table tbody tr:nth-child(even) {
background-color: #ecf5ff;
}


/* 鼠标悬停效果 */
table tbody tr:hover {
background-color: #d5fdeb;
cursor: pointer;
}

/* 链接点击效果 */
a:link {
color: #0870f5 !important; /* 强制未点击链接为蓝色 */
}
a:visited {
color: #F56C6C !important; /* 强制已点击链接为紫色 */
}



</style>
`
