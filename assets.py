import dagster as dg

print(dg)

@dg.asset
def simple(context: dg.AssetExecutionContext):
    context.log.info("A simple asset")



@dg.asset(deps=[simple])
def second(context: dg.AssetExecutionContext):
    context.log.info("second asset")    



@dg.asset(deps=[second])
def third(context: dg.AssetExecutionContext):
    context.log.info("third asset")  

# dagster definitions  
# 
dagsterdefs = dg.Definitions(assets=[simple, second, third])       

